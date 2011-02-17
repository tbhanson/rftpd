#|

Racket FTP Server v1.1.6
----------------------------------------------------------------------

Summary:
This file is part of Racket FTP Server.

License:
Copyright (c) 2010-2011 Mikhail Mosienko <netluxe@gmail.com>
All Rights Reserved

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
|#

#lang racket

(require ffi/unsafe
         (file "lib-ssl.rkt")
         (prefix-in ftp: (file "lib-rftpd.rkt")))

(define-values (ShowConsole HideConsole)
  (case (system-type) 
    [(windows)
     (define _HWND (_or-null _pointer))
     (define SW_HIDE 0)
     (define SW_SHOW 5)
     
     (define-c GetConsoleWindow "Kernel32.dll" (_fun -> _HWND))
     (define-c ShowWindow "User32.dll" (_fun _HWND _int -> _bool))
     
     (values (λ() (ShowWindow (GetConsoleWindow) SW_SHOW))
             (λ() (ShowWindow (GetConsoleWindow) SW_HIDE)))]
    [else
     (values void void)]))

(define racket-ftp-server%
  (class object%
    (super-new)
    
    (init-field [server-name&version     "Racket FTP Server v1.1.6 <development>"]
                [copyright               "Copyright (c) 2010-2011 Mikhail Mosienko <cnet@land.ru>"]
                [ci-help-msg             "Type 'help' or '?' for help."]
                
                [read-cmd-line?          #f]
                [ci-interactive?         #f]
                [show-banner?            #f]
                [echo?                   #f]
                
                [server-1-host           "127.0.0.1"]
                [server-1-port           21]
                [server-1-encryption     #f]
                [server-1-certificate    "certs/server-1.pem"]
                
                [server-2-host           #f]
                [server-2-port           21]
                [server-2-encryption     #f]
                [server-2-certificate    "certs/server-2.pem"]
                
                [server-max-allow-wait   50]
                
                [passive-1-ports         (ftp:make-passive-ports 40000 40599)]
                [passive-2-ports         (ftp:make-passive-ports 40000 40599)]
                
                [control-passwd          "12345"]
                [control-host            "127.0.0.1"]
                [control-port            40600]
                [control-encryption      'sslv3]
                [control-certificate     "certs/control.pem"]
                
                [default-locale-encoding "UTF-8"]
                [default-root-dir        "ftp-dir"]
                
                [log-file                "logs/ftp.log"]
                
                [config-file             "conf/ftp.config"]
                [users-file              "conf/ftp.users"]
                
                [bad-admin-auth-sleep-sec 120]
                [max-admin-pass-attempts  5])
    
    (define log-out #f)
    (define server #f)
    (define bad-admin-auth (cons 0 0)) ; (cons attempts time) 
    ;;
    ;; ---------- Public Methods ----------
    ;;
    (define/public (main)
      (let ([shutdown? #f]
            [start? #f]
            [pause?  #f]
            [restart? #f]
            [cust (make-custodian)])
        (with-handlers ([exn:fail:network? 
                         (λ(e)
                           (when (or start? restart?
                                     (equal? (current-command-line-arguments) #()))
                             (start-server)))]
                        [any/c void])
          (when read-cmd-line?
            (command-line
             #:program "rftpd"
             #:once-any
             [("-r" "--start")
              "Start server"
              (set! start? #t)]
             [("-p" "--pause")
              "Pause server"
              (set! pause? #t)]
             [("-t" "--restart")
              "Restart server"
              (set! restart? #t)]
             [("-s" "--stop" "-x" "--exit")
              "Shutdown server"  
              (set! shutdown? #t)]
             #:once-each
             [("-i" "--interactive")
              "Start a RFTPd Remote Control Interface in interactive mode"  
              (set! ci-interactive? #t)]
             [("-v" "--version")
              "Shows version and copyright" 
              (set! show-banner? #t)]
             [("-e" "--echo")
              "Show echo"
              (set! echo? #t)]
             ["--path"
              path
              ("Change current directory" "Syntax: --path \"path\"")
              (current-directory path)]))
          (unless (or show-banner? echo? ci-interactive?) 
            (HideConsole))
          (when show-banner?
            (display-lines (list server-name&version copyright "")))
          (init)
          (parameterize ([current-custodian cust])
            (let*-values ([(ssl-ctx) (let ([ctx (ssl-make-client-context control-encryption)])
                                       (ssl-load-certificate-chain! ctx control-certificate default-locale-encoding)
                                       (ssl-load-private-key! ctx control-certificate #t #f default-locale-encoding)
                                       ctx)]
                          [(in out) (ssl-connect control-host control-port ssl-ctx)])
              (displayln control-passwd out)(flush-output out)
              (when (string=? (read-line in) "Ok")
                (let ([request (λ (cmd)
                                 (displayln cmd out)
                                 (flush-output out)
                                 (displayln (read-line in)))])
                  (cond
                    (start?
                     (request "%start")
                     (request "%bye"))
                    (pause?
                     (request "%pause")
                     (request "%bye"))
                    (restart?
                     (request "%restart")
                     (request "%bye"))
                    (shutdown?
                     (request "%exit")
                     (request "%bye"))
                    (else
                     (when ci-interactive?
                       (displayln ci-help-msg) (newline)
                       (let loop ()
                         (display "#> ")
                         (let ([cmd (read)])
                           (case cmd
                             ((help ?)
                              (display-lines '("%start   - start server."
                                               "%pause   - pause server."
                                               "%restart - restart server."
                                               "%exit    - shutdown server."
                                               "%bye     - close session."))
                              (loop))
                             (else
                              (request cmd)
                              (unless (memq cmd '(%exit %bye))
                                (loop))))))))))))
            (custodian-shutdown-all cust)))))
    ;;
    ;; ---------- Private Methods ----------
    ;;
    (define/private (start-server)
      (set! server (new ftp:ftp-server% 
                        [welcome-message server-name&version]
                        [server-1-host server-1-host]
                        [server-1-port server-1-port]
                        [server-1-encryption server-1-encryption]
                        [server-1-certificate server-1-certificate]
                        [server-2-host server-2-host]
                        [server-2-port server-2-port]
                        [server-2-encryption server-2-encryption]
                        [server-2-certificate server-2-certificate]
                        [max-allow-wait server-max-allow-wait]
                        [passive-1-ports passive-1-ports]
                        [passive-2-ports passive-2-ports]
                        [default-root-dir default-root-dir]
                        [default-locale-encoding default-locale-encoding]
                        [log-output-port log-out]))
      (load-users)
      (start!)
      (server-control)
      (let loop ()
        (sleep 60)
        (loop)))
    
    (define/private (server-control)
      (let ([cust (make-custodian)])
        (parameterize ([current-custodian cust])
          (let ([listener (ssl-listen control-port (random 123456789) #t control-host control-encryption)])
            (ssl-load-certificate-chain! listener control-certificate default-locale-encoding)
            (ssl-load-private-key! listener control-certificate #t #f default-locale-encoding)
            (letrec ([main-loop (λ ()
                                  (handle-client-request listener)
                                  (main-loop))])
              (thread main-loop))))))
    
    (define/private (handle-client-request listener)
      (let-values ([(in out) (ssl-accept listener)])
        (thread (λ ()
                  (with-handlers ([any/c #|displayln|# void])
                    (eval-cmd in out)
                    (close-input-port in)
                    (close-output-port out))))))
    
    (define/private (eval-cmd input-port output-port)
      (let ([pass (read-line input-port)])
        (if (and (or ((car bad-admin-auth). < . max-admin-pass-attempts)
                     (> ((current-seconds). - .(cdr bad-admin-auth))
                        bad-admin-auth-sleep-sec))
                 (string=? pass control-passwd))
            (let ([response (λ msg
                              (apply fprintf output-port msg)
                              (newline output-port)
                              (flush-output output-port))])
              (set! bad-admin-auth (cons 0 0))
              (response "Ok")
              (let next ([cmd (read input-port)])
                (cond
                  ((eof-object? cmd))
                  ((eq? cmd '%bye)
                   (response "Ok"))
                  ((eq? cmd '%exit)
                   (pause!)
                   (close-output-port log-out)
                   (response "Ok")
                   (exit))
                  ((eq? cmd '%start)
                   (start!)
                   (response "Ok")
                   (next (read input-port)))
                  ((eq? cmd '%pause)
                   (flush-output log-out)
                   (pause!)
                   (response "Ok")
                   (next (read input-port)))
                  ((eq? cmd '%restart)
                   (flush-output log-out)
                   (pause!)
                   (start!)
                   (response "Ok")
                   (next (read input-port)))
                  (else
                   (response "Command '~a' not implemented.\n" cmd)
                   (next (read input-port))))))
            (set! bad-admin-auth (cons (add1 (car bad-admin-auth))
                                       (current-seconds))))))
    
    (define/private (start!)
      (send server start)
      (when echo? (displayln (format "Server ~a!" (send server status)))))
    
    (define/private (pause!)
      (send server stop)
      (when echo? (displayln (format "Server ~a!" (send server status)))))
    
    (define/private (load-config)
      (with-handlers ([any/c void])
        (call-with-input-file config-file
          (λ (in)
            (let ([conf (read in)])
              (when (eq? (car conf) 'ftp-server-config)
                (for-each (λ (param)
                            (case (car param)
                              ((server-1)
                               (for-each (λ (param)
                                           (with-handlers ([any/c void])
                                             (case (car param)
                                               ((host&port)
                                                (set! server-1-host 
                                                      (and (ftp:host-string? (second param)) (second param)))
                                                (set! server-1-port 
                                                      (and (ftp:port-number? (third param)) (third param))))
                                               ((ssl-protocol&certificate)
                                                (set! server-1-encryption 
                                                      (and (ftp:ssl-protocol? (second param)) (second param)))
                                                (set! server-1-certificate (third param)))
                                               ((passive-ports)
                                                (set! passive-1-ports 
                                                      (ftp:make-passive-ports (second param) (third param)))))))
                                         (cdr param)))
                              ((server-2)
                               (for-each (λ (param)
                                           (with-handlers ([any/c void])
                                             (case (car param)
                                               ((host&port)
                                                (set! server-2-host 
                                                      (and (ftp:host-string? (second param)) (second param)))
                                                (set! server-2-port 
                                                      (and (ftp:port-number? (third param)) (third param))))
                                               ((ssl-protocol&certificate)
                                                (set! server-2-encryption 
                                                      (and (ftp:ssl-protocol? (second param)) (second param)))
                                                (set! server-2-certificate (third param)))
                                               ((passive-ports)
                                                (set! passive-2-ports 
                                                      (ftp:make-passive-ports (second param) (third param)))))))
                                         (cdr param)))
                              ((control-server)
                               (for-each (λ (param)
                                           (with-handlers ([any/c void])
                                             (case (car param)
                                               ((host&port)
                                                (set! control-host 
                                                      (and (ftp:host-string? (second param)) (second param)))
                                                (set! control-port 
                                                      (and (ftp:port-number? (third param)) (third param))))
                                               ((ssl-protocol&certificate)
                                                (set! control-encryption 
                                                      (and (ftp:ssl-protocol? (second param)) (second param)))
                                                (set! control-certificate (third param)))
                                               ((passwd)
                                                (set! control-passwd (second param))))))
                                         (cdr param)))
                              ((max-allow-wait)
                               (set! server-max-allow-wait (second param)))
                              ((default-locale-encoding)
                               (set! default-locale-encoding (second param)))
                              ((default-root-dir)
                               (set! default-root-dir (second param)))
                              ((log-file)
                               (when log-out (close-output-port log-out))
                               (set! log-out (open-output-file (second param) #:exists 'append)))))
                          (cdr conf))))))))
    
    (define/private (load-users)
      (with-handlers ([any/c void])
        (call-with-input-file users-file
          (λ (in)
            (let ([conf (read in)])
              (when (eq? (car conf) 'ftp-server-users)
                (for-each (λ (user)
                            (send server add-ftp-user
                                  (car user) (second user) (third user) (fourth user) (fifth user) (sixth user)))
                          (cdr conf))))))))
    
    (define/private (init)
      (load-config)
      (unless log-out
        (set! log-out (open-output-file log-file #:exists 'append))))))

;-----------------------------------
;              BEGIN
;-----------------------------------
(send (new racket-ftp-server% [read-cmd-line? #t]) main)

