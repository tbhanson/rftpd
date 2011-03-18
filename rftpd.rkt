#|

Racket FTP Server v1.2.5
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
         racket/date
         (file "lib-ssl.rkt")
         (prefix-in ftp: (file "lib-rftpd.rkt")))

(define-for-syntax DrRacket-DEBUG? #t)

(date-display-format 'iso-8601)

(define-syntax-rule (format-file-name spath)
  (regexp-replace #rx"\\*" spath (date->string (current-date))))

(define-syntax (if-drdebug so)
  (syntax-case so ()
    [(_ exp1 exp2)
     (if DrRacket-DEBUG? #'exp1 #'exp2)]))

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

(define run-dir-path (path-only (find-system-path 'run-file)))

(define (build-rtm-path path)
  (if-drdebug 
   path
   (if (or (eq? (string-ref path 0) #\/)
           (not run-dir-path))
       path
       (build-path run-dir-path path))))

(define-syntax (os-build-rtm-path so)
  (syntax-case so ()
    [(_ path)
     (if (eq? (system-type) 'windows)
         #'path
         #'(build-rtm-path (string-append "../" path)))]))

(define racket-ftp-server%
  (class object%
    (super-new)
    
    (init-field [server-name&version        "Racket FTP Server v1.2.5 <development>"]
                [copyright                  "Copyright (c) 2010-2011 Mikhail Mosienko <netluxe@gmail.com>"]
                [ci-help-msg                "Type 'help' or '?' for help."]
                
                [read-cmd-line?             #f]
                [ci-interactive?            #f]
                [show-banner?               #f]
                [echo?                      #f]
                
                [server-1-host              #f]
                [server-1-port              21]
                [server-1-encryption        #f]
                [server-1-certificate       (os-build-rtm-path "certs/server-1.pem")]
                
                [server-2-host              #f]
                [server-2-port              21]
                [server-2-encryption        #f]
                [server-2-certificate       (os-build-rtm-path "certs/server-2.pem")]
                
                [server-max-allow-wait      25]
                [server-transfer-wait-time  120]
                
                [server-bad-auth-sleep-sec  60]
                [server-max-auth-attempts   5]
                [server-passwd-sleep-sec    0]
                
                [disable-ftp-commands       null]
                
                [passive-1-host&ports       (ftp:make-passive-host&ports "127.0.0.1" 40000 40999)]
                [passive-2-host&ports       (ftp:make-passive-host&ports "127.0.0.1" 40000 40999)]
                
                [control-passwd             "12345"]
                [control-host               "127.0.0.1"]
                [control-port               41234]
                [control-encryption         'sslv3]
                [control-certificate        (os-build-rtm-path "certs/control.pem")]
                
                [default-locale-encoding    "UTF-8"]
                [default-root-dir           "ftp-dir"]
                
                [log-file                   (os-build-rtm-path (format-file-name "logs/rftpd.log"))]
                
                [config-file                (os-build-rtm-path "conf/rftpd.conf")]
                [users-file                 (os-build-rtm-path "conf/rftpd.users")]
                [groups-file                (os-build-rtm-path "conf/rftpd.groups")]
                
                [bad-admin-auth-sleep-sec   120]
                [max-admin-passwd-attempts  5])
    
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
              (set! echo? #t)]))
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
                        [transfer-wait-time server-transfer-wait-time]
                        [bad-auth-sleep-sec server-bad-auth-sleep-sec]
                        [max-auth-attempts server-max-auth-attempts]
                        [pass-sleep-sec server-passwd-sleep-sec]
                        [disable-ftp-commands disable-ftp-commands]
                        [passive-1-host&ports passive-1-host&ports]
                        [passive-2-host&ports passive-2-host&ports]
                        [default-root-dir default-root-dir]
                        [default-locale-encoding default-locale-encoding]
                        [log-output-port log-out]))
      (load-users)
      (load-groups)
      (start!)
      (thread-wait (server-control)))
    
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
        (if (and (or ((car bad-admin-auth). < . max-admin-passwd-attempts)
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
                                                (set! server-1-certificate (build-rtm-path (third param))))
                                               ((passive-host&ports)
                                                (set! passive-1-host&ports 
                                                      (ftp:make-passive-host&ports (second param) 
                                                                                   (third param)
                                                                                   (fourth param)))))))
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
                                                (set! server-2-certificate (build-rtm-path (third param))))
                                               ((passive-host&ports)
                                                (set! passive-2-host&ports 
                                                      (ftp:make-passive-host&ports (second param) 
                                                                                   (third param)
                                                                                   (fourth param)))))))
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
                                                (set! control-certificate (build-rtm-path (third param))))
                                               ((passwd)
                                                (set! control-passwd (second param)))
                                               ((bad-admin-auth-sleep-sec)
                                                (set! bad-admin-auth-sleep-sec (second param)))
                                               ((max-admin-passwd-attempts)
                                                (set! max-admin-passwd-attempts (second param))))))
                                         (cdr param)))
                              ((max-allow-wait)
                               (set! server-max-allow-wait (second param)))
                              ((transfer-wait-time)
                               (set! server-transfer-wait-time (second param)))
                              ((bad-auth-sleep-sec)
                               (set! server-bad-auth-sleep-sec (second param)))
                              ((max-auth-attempts)
                               (set! server-max-auth-attempts (second param)))
                              ((passwd-sleep-sec)
                               (set! server-passwd-sleep-sec (second param)))
                              ((disable-ftp-commands)
                               (set! disable-ftp-commands (second param)))
                              ((default-locale-encoding)
                               (set! default-locale-encoding (second param)))
                              ((default-root-dir)
                               (set! default-root-dir (second param)))
                              ((log-file)
                               (when log-out (close-output-port log-out))
                               (set! log-out (open-output-file 
                                              (build-rtm-path (format-file-name (second param))) 
                                              #:exists 'append)))))
                          (cdr conf))))))))
    
    (define/private (load-users)
      (with-handlers ([any/c #|displayln|# void])
        (call-with-input-file users-file
          (λ (in)
            (let ([conf (read in)])
              (when (eq? (car conf) 'ftp-server-users)
                (for-each (λ (user)
                            (send server useradd
                                  (car user) (second user) (third user) 
                                  (fourth user) (fifth user) (sixth user) (seventh user)))
                          (cdr conf))))))))
    
    (define/private (load-groups)
      (with-handlers ([any/c #|displayln|# void])
        (call-with-input-file groups-file
          (λ (in)
            (let ([conf (read in)])
              (when (eq? (car conf) 'ftp-server-groups)
                (for-each (λ (group)
                            (send server groupadd (car group) (cadr group) (cddr group)))
                          (cdr conf))))))))
    
    (define/private (init)
      (load-config)
      (unless log-out
        (set! log-out (open-output-file log-file #:exists 'append))))))

;-----------------------------------
;              BEGIN
;-----------------------------------
(send (new racket-ftp-server% [read-cmd-line? #t]) main)

