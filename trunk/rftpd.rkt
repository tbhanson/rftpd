#|

Racket FTP Server v1.2.7
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
         (file "debug.rkt")
         (file "lib-ssl.rkt")
         (file "utils.rkt")
         (prefix-in ftp: (file "lib-rftpd.rkt")))

(date-display-format 'iso-8601)

(struct ftp-srv-params
  (welcome-message
   host
   port
   encryption 
   certificate 
   max-allow-wait
   transfer-wait-time
   bad-auth-sleep-sec
   max-auth-attempts 
   passwd-sleep-sec 
   disable-ftp-commands
   passive-host&ports 
   default-root-dir
   log-file
   users-file
   groups-file))

(define-syntax-rule (format-file-name spath)
  (regexp-replace #rx"\\*" spath (date->string (current-date))))

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
    
    (init-field [server-name&version        "Racket FTP Server v1.2.7 <development>"]
                [copyright                  "Copyright (c) 2010-2011 Mikhail Mosienko <netluxe@gmail.com>"]
                [ci-help-msg                "Type 'help' or '?' for help."]
                
                [read-cmd-line?             #f]
                [ci-interactive?            #f]
                [show-banner?               #f]
                [echo?                      #f]
                
                [control-passwd             "12345"]
                [control-host               "127.0.0.1"]
                [control-port               41234]
                [control-encryption         'sslv3]
                [control-certificate        (os-build-rtm-path "certs/control.pem")]
                [bad-admin-auth-sleep-sec   120]
                [max-admin-passwd-attempts  5]
                
                [config-file                (os-build-rtm-path "conf/rftpd.conf")]
                
                [default-locale-encoding    "UTF-8"])
    
    (define bad-admin-auth (cons 0 0)) ; (cons attempts time) 
    (define ftp-servers-params (make-hash))
    (define ftp-servers #f)
    ;;
    ;; ---------- Public Methods ----------
    ;;
    (define/public (main)
      (let ([shutdown? #f]
            [start? #f]
            [stop?  #f]
            [restart? #f]
            [cust (make-custodian)])
        (with-handlers ([exn:fail:network? 
                         (λ(e)
                           (when (or start? restart?
                                     (equal? (current-command-line-arguments) #()))
                             (start-servers)))]
                        [any/c void])
          (when read-cmd-line?
            (command-line
             #:program "rftpd"
             #:once-any
             [("-r" "--start")
              "Start all servers."
              (set! start? #t)]
             [("-p" "--stop")
              "Stop all servers."
              (set! stop? #t)]
             [("-t" "--restart")
              "Restart all server."
              (set! restart? #t)]
             [("-s" "--shutdown" "-x" "--exit")
              "Shutdown RFTPd server."  
              (set! shutdown? #t)]
             #:once-each
             [("-i" "--interactive")
              "Start a RFTPd Remote Control Interface in interactive mode."  
              (set! ci-interactive? #t)]
             [("-v" "--version")
              "Shows version and copyright." 
              (set! show-banner? #t)]
             [("-e" "--echo")
              "Show echo."
              (set! echo? #t)]))
          (unless (or show-banner? echo? ci-interactive?) 
            (HideConsole))
          (when show-banner?
            (display-lines (list server-name&version copyright "")))
          (load-config config-file)
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
                    (stop?
                     (request "%pause")
                     (request "%bye"))
                    (restart?
                     (request "%restart")
                     (request "%bye"))
                    (shutdown?
                     (request "%shutdown")
                     (request "%bye"))
                    (else
                     (when ci-interactive?
                       (displayln ci-help-msg) (newline)
                       (let loop ()
                         (display "#> ")
                         (let ([cmd (read)])
                           (case cmd
                             ((help ?)
                              (display-lines '("(%start ID)   - start ID server."
                                               "(%pause ID)   - stop ID server."
                                               "(%stop ID)    - stop ID server."
                                               "(%restart ID) - restart ID server."
                                               "%start        - start all servers."
                                               "%pause        - stop all servers."
                                               "%stop         - stop all servers."
                                               "%restart      - restart all servers."
                                               "%shutdown     - shutdown RFTPd server."
                                               "%bye          - close session."))
                              (loop))
                             (else
                              (request cmd)
                              (unless (memq cmd '(%exit %bye))
                                (loop))))))))))))
            (custodian-shutdown-all cust)))))
    ;;
    ;; ---------- Private Methods ----------
    ;;
    (define/private (start-servers)
      (set! ftp-servers (make-hash))
      (hash-for-each
       ftp-servers-params
       (λ (id params)
         (let ([srv (new ftp:ftp-server%
                         [welcome-message         (ftp-srv-params-welcome-message params)]
                         
                         [server-host             (ftp-srv-params-host params)]
                         [server-port             (ftp-srv-params-port params)]
                         [server-encryption       (ftp-srv-params-encryption params)]
                         [server-certificate      (ftp-srv-params-certificate params)]
                         
                         [max-allow-wait          (ftp-srv-params-max-allow-wait params)]
                         [transfer-wait-time      (ftp-srv-params-transfer-wait-time params)]
                         
                         [bad-auth-sleep-sec      (ftp-srv-params-bad-auth-sleep-sec params)]
                         [max-auth-attempts       (ftp-srv-params-max-auth-attempts params)]
                         [pass-sleep-sec          (ftp-srv-params-passwd-sleep-sec params)]
                         
                         [disable-ftp-commands    (ftp-srv-params-disable-ftp-commands params)]
                         
                         [pasv-host&ports         (ftp-srv-params-passive-host&ports params)]
                         
                         [default-root-dir        (ftp-srv-params-default-root-dir params)]
                         [default-locale-encoding default-locale-encoding]
                         [log-file                (ftp-srv-params-log-file params)])])
           (hash-set! ftp-servers id srv)
           (start! id srv))))
      (thread-wait (server-control)))
    
    (define/private (server-control)
      (let ([listener (ssl-listen control-port (random 123456789) #t control-host control-encryption)])
        (ssl-load-certificate-chain! listener control-certificate default-locale-encoding)
        (ssl-load-private-key! listener control-certificate #t #f default-locale-encoding)
        (letrec ([main-loop (λ ()
                              (handle-client-request listener)
                              (main-loop))])
          (thread main-loop))))
    
    (define/private (handle-client-request listener)
      (let ([cust (make-custodian)])
        (parameterize ([current-custodian cust])
          (let-values ([(in out) (ssl-accept listener)])
            (thread (λ ()
                      (with-handlers ([any/c debug/handler])
                        (eval-cmd in out))
                      (custodian-shutdown-all cust)))))))
    
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
                (unless (eof-object? cmd)
                  (match cmd
                    ['%bye 
                     (response "Ok")]
                    ['%start 
                     (start!-all)
                     (response "Ok")
                     (next (read input-port))]
                    [(or '%stop '%pause)
                     (stop!-all)
                     (response "Ok")
                     (next (read input-port))]
                    ['%restart 
                     (stop!-all)
                     (start!-all)
                     (response "Ok")
                     (next (read input-port))]
                    [(or '%shutdown '%exit)
                     (stop!-all)
                     (response "Ok")
                     (exit)]
                    [`(%start ,id)
                     (if (hash-ref ftp-servers id #f)
                         (begin
                           (start! id (hash-ref ftp-servers id))
                           (response "Ok"))
                         (response "Server ID not found."))
                     (next (read input-port))]
                    [(or `(%pause ,id) `(%stop ,id))
                     (if (hash-ref ftp-servers id #f)
                         (begin
                           (stop! id (hash-ref ftp-servers id))
                           (response "Ok"))
                         (response "Server ID not found."))
                     (response "Ok")
                     (next (read input-port))]
                    [`(%restart ,id)
                     (if (hash-ref ftp-servers id #f)
                         (begin
                           (stop! id (hash-ref ftp-servers id))
                           (start! id (hash-ref ftp-servers id))
                           (response "Ok"))
                         (response "Server ID not found."))
                     (response "Ok")
                     (next (read input-port))]
                    [_
                     (if (and (pair? cmd) (list? cmd))
                         (if (memq (car cmd) '(%start %stop %pause %restart))
                             (response "Syntax error.")
                             (response "Command '~a' not implemented." (car cmd)))
                         (response "Command '~a' not implemented." cmd))
                     (next (read input-port))]))))
            (set! bad-admin-auth (cons (add1 (car bad-admin-auth))
                                       (current-seconds))))))
    
    (define/private (start! id server)
      (send server clear-users&groups-tables)
      (load-users server (ftp-srv-params-users-file (hash-ref ftp-servers-params id)))
      (load-groups server (ftp-srv-params-groups-file (hash-ref ftp-servers-params id)))
      (send server start)
      (when echo? (displayln (format "Server ~a: ~a!" id (send server status)))))
    
    (define/private (stop! id server)
      (send server stop)
      (when echo? (displayln (format "Server ~a: ~a!" id (send server status)))))
    
    (define/private (start!-all)
      (hash-for-each ftp-servers (λ (id srv) (start! id srv))))
    
    (define/private (stop!-all)
      (hash-for-each ftp-servers (λ (id srv) (stop! id srv))))
    
    (define/private (load-config config-file)
      (with-handlers ([any/c debug/handler])
        (call-with-input-file config-file
          (λ (in)
            (let ([conf (read in)])
              (when (eq? (car conf) 'ftp-server-config)
                (for-each 
                 (λ (param)
                   (case (car param)
                     [(server)
                      (with-handlers ([any/c debug/handler])
                        (let ([id                    #f]
                              [welcome-message       server-name&version]
                              [host                  #f]
                              [port                  21]
                              [encryption            #f]
                              [certificate           (os-build-rtm-path "certs/server-1.pem")]
                              [max-allow-wait        25]
                              [transfer-wait-time    120]
                              ;====================
                              [bad-auth-sleep-sec    60]
                              [max-auth-attempts     5]
                              [passwd-sleep-sec      0]
                              ;====================
                              [disable-ftp-commands  null]
                              ;====================
                              [passive-host&ports    (ftp:make-passive-host&ports "127.0.0.1" 40000 40999)]
                              ;====================
                              [default-root-dir      "ftp-dir"]
                              ;====================
                              [log-file              (os-build-rtm-path (format-file-name "logs/rftpd.log"))]
                              [users-file            (os-build-rtm-path "conf/rftpd.users")]
                              [groups-file           (os-build-rtm-path "conf/rftpd.groups")])
                          (if (or (symbol? (second param))
                                  (number? (second param)))
                              (set! id (second param))
                              (error 'id))
                          (for-each 
                           (λ (param)
                             (case (car param)
                               ((welcome-message)
                                (set! welcome-message (second param)))
                               ((host&port)
                                (set! host (and (ftp:host-string? (second param)) (second param)))
                                (set! port (and (ftp:port-number? (third param)) (third param))))
                               ((ssl-protocol&certificate)
                                (set! encryption (and (ftp:ssl-protocol? (second param)) (second param)))
                                (set! certificate (build-rtm-path (third param))))
                               ((passive-host&ports)
                                (set! passive-host&ports 
                                      (ftp:make-passive-host&ports (second param) 
                                                                   (third param)
                                                                   (fourth param))))
                               ((max-allow-wait)
                                (set! max-allow-wait (second param)))
                               ((transfer-wait-time)
                                (set! transfer-wait-time (second param)))
                               ((bad-auth-sleep-sec)
                                (set! bad-auth-sleep-sec (second param)))
                               ((max-auth-attempts)
                                (set! max-auth-attempts (second param)))
                               ((passwd-sleep-sec)
                                (set! passwd-sleep-sec (second param)))
                               ((disable-ftp-commands)
                                (set! disable-ftp-commands (second param)))
                               ((default-root-dir)
                                (set! default-root-dir (second param)))
                               ((log-file)
                                (set! log-file (build-rtm-path (format-file-name (second param)))))))
                           (cddr param))
                          (hash-set! ftp-servers-params id (ftp-srv-params welcome-message
                                                                           host
                                                                           port
                                                                           encryption 
                                                                           certificate 
                                                                           max-allow-wait
                                                                           transfer-wait-time
                                                                           bad-auth-sleep-sec
                                                                           max-auth-attempts 
                                                                           passwd-sleep-sec 
                                                                           disable-ftp-commands
                                                                           passive-host&ports 
                                                                           default-root-dir
                                                                           log-file
                                                                           users-file
                                                                           groups-file))))]
                     [(control-server)
                      (for-each 
                       (λ (param)
                         (with-handlers ([any/c void])
                           (case (car param)
                             ((host&port)
                              (set! control-host (and (ftp:host-string? (second param)) (second param)))
                              (set! control-port (and (ftp:port-number? (third param)) (third param))))
                             ((ssl-protocol&certificate)
                              (set! control-encryption (and (ftp:ssl-protocol? (second param)) (second param)))
                              (set! control-certificate (build-rtm-path (third param))))
                             ((passwd)
                              (set! control-passwd (second param)))
                             ((bad-admin-auth-sleep-sec)
                              (set! bad-admin-auth-sleep-sec (second param)))
                             ((max-admin-passwd-attempts)
                              (set! max-admin-passwd-attempts (second param))))))
                       (cdr param))]
                     [(default-locale-encoding)
                      (set! default-locale-encoding (second param))]))
                 (cdr conf))))))))
    
    (define/private (load-users server users-file)
      (with-handlers ([any/c debug/handler])
        (call-with-input-file users-file
          (λ (in)
            (let ([conf (read in)])
              (when (eq? (car conf) 'ftp-server-users)
                (for-each (λ (user)
                            (send server useradd
                                  (car user) (second user) (third user) 
                                  (fourth user) (fifth user) (sixth user) (seventh user)))
                          (cdr conf))))))))
    
    (define/private (load-groups server groups-file)
      (with-handlers ([any/c debug/handler])
        (call-with-input-file groups-file
          (λ (in)
            (let ([conf (read in)])
              (when (eq? (car conf) 'ftp-server-groups)
                (for-each (λ (group)
                            (send server groupadd (car group) (cadr group) (cddr group)))
                          (cdr conf))))))))))

;-----------------------------------
;              BEGIN
;-----------------------------------
(send (new racket-ftp-server% [read-cmd-line? #t]) main)
