#|

ProRFTPd v1.0.6
----------------------------------------------------------------------

Summary:
This file is part of ProRFTPd.

License:
Copyright (c) 2011 Mikhail Mosienko <netluxe@gmail.com>
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

(require racket/date
         (file "debug.rkt")
         (for-syntax (file "debug.rkt"))
         (file "lib-ssl.rkt")
         (file "utils.rkt")
         (file "system.rkt")
         (file "platform.rkt")
         (prefix-in ftp: (file "lib-prorftpd.rkt")))

(struct ftp-srv-params
  (welcome-message
   host
   port
   ssl-protocol
   ssl-key
   ssl-certificate 
   max-allow-wait
   port-timeout
   pasv-timeout
   data-timeout
   session-timeout
   max-clients-per-IP
   bad-auth-sleep-sec
   max-auth-attempts 
   passwd-sleep-sec 
   hide-dotfiles?
   text-user&group-names?
   hide-ids?
   pasv-enable?
   port-enable?
   read-only?
   disable-ftp-commands
   allow-foreign-address
   passive-host&ports 
   default-root-dir
   log-file
   users-file))

(define-syntax-rule (format-file-name spath)
  (regexp-replace #rx"\\*" spath (date->string (current-date))))

(define racket-ftp-server%
  (class object%
    (super-new)
    
    (init-field [server-name&version        "ProRFTPd v1.0.6 <development>"]
                [copyright                  "Copyright (c) 2011 Mikhail Mosienko <netluxe@gmail.com>"]
                [ci-help-msg                "Type 'help' or '?' for help."]
                
                [read-cmd-line?             #f]
                [ci-interactive?            #f]
                [show-banner?               #f]
                [echo?                      #f]
                
                [control-host               "127.0.0.1"]
                [control-port               41234]
                [control-protocol           'sslv3]
                [control-key                "../certs/control.pem"]
                [control-certificate        "../certs/control.pem"]
                [bad-admin-auth-sleep-sec   120]
                [max-admin-passwd-attempts  5]
                
                [config-file                "../conf/prorftpd.conf"]
                
                [default-locale-encoding    "UTF-8"])
    
    (define bad-admin-auth (cons 0 0)) ; (cons attempts time) 
    (define ftp-servers-params (make-hash))
    (define ftp-servers #f)
    (define control-admin (uid->uname (getuid)))
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
                        [any/c debug/handler])
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
              "Shutdown ProRFTPd server."  
              (set! shutdown? #t)]
             #:once-each
             [("-i" "--interactive")
              "Start a ProRFTPd Control Interface in interactive mode."  
              (set! ci-interactive? #t)]
             [("-v" "--version")
              "Shows version and copyright." 
              (set! show-banner? #t)]
             [("-e" "--echo")
              "Show echo."
              (set! echo? #t)]
             [("-f" "--config") file-path
                                "Use an alternate configuration file."
                                (set! config-file file-path)]))
          (when show-banner?
            (display-lines (list server-name&version copyright "")))
          (load-config config-file)
          (parameterize ([current-custodian cust])
            (let*-values ([(hash-pass) (crypt-string (get-shadow-passwd control-admin)
                                                     (string-append "$1$" control-admin))]
                          [(ssl-ctx) (let ([ctx (ssl-make-client-context control-protocol)])
                                       (ssl-load-certificate-chain! ctx control-certificate default-locale-encoding)
                                       (ssl-load-private-key! ctx control-key #t #f default-locale-encoding)
                                       ctx)]
                          [(in out) (ssl-connect control-host control-port ssl-ctx)])
              (displayln hash-pass out) (flush-output out)
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
      (unless-drdebug
       (activate-daemon-mode #t #t))
      (set! ftp-servers (make-hash))
      (hash-for-each
       ftp-servers-params
       (λ (id params)
         (with-handlers ([any/c (λ(e) 
                                  (when-drdebug (displayln e))
                                  (print-error "Please check your settings for the server '~a'." id))])
           (let ([srv (new ftp:ftp-server%
                           [welcome-message         (ftp-srv-params-welcome-message params)]
                           
                           [server-host             (ftp-srv-params-host params)]
                           [server-port             (ftp-srv-params-port params)]
                           [ssl-protocol            (ftp-srv-params-ssl-protocol params)]
                           [ssl-key                 (ftp-srv-params-ssl-key params)]
                           [ssl-certificate         (ftp-srv-params-ssl-certificate params)]
                           
                           [max-allow-wait          (ftp-srv-params-max-allow-wait params)]
                           [port-timeout            (ftp-srv-params-port-timeout params)]
                           [pasv-timeout            (ftp-srv-params-pasv-timeout params)]
                           [data-timeout            (ftp-srv-params-data-timeout params)]
                           [session-timeout         (ftp-srv-params-session-timeout params)]
                           [max-clients-per-IP      (ftp-srv-params-max-clients-per-IP params)]
                           
                           [bad-auth-sleep-sec      (ftp-srv-params-bad-auth-sleep-sec params)]
                           [max-auth-attempts       (ftp-srv-params-max-auth-attempts params)]
                           [pass-sleep-sec          (ftp-srv-params-passwd-sleep-sec params)]
                           
                           [hide-dotfiles?          (ftp-srv-params-hide-dotfiles? params)]
                           [text-user&group-names?  (ftp-srv-params-text-user&group-names? params)]
                           [hide-ids?               (ftp-srv-params-hide-ids? params)]
                           [pasv-enable?            (ftp-srv-params-pasv-enable? params)]
                           [port-enable?            (ftp-srv-params-port-enable? params)]
                           [read-only?              (ftp-srv-params-read-only? params)]
                           [disable-ftp-commands    (ftp-srv-params-disable-ftp-commands params)]
                           
                           [allow-foreign-address   (ftp-srv-params-allow-foreign-address params)]
                           
                           [pasv-host&ports         (ftp-srv-params-passive-host&ports params)]
                           
                           [default-root-dir        (ftp-srv-params-default-root-dir params)]
                           [default-locale-encoding default-locale-encoding]
                           [log-file                (ftp-srv-params-log-file params)])])
             (hash-set! ftp-servers id srv)
             (start! id srv)))))
      (when (positive? (hash-count ftp-servers-params))
        (unless-drdebug
         (unless echo?
           (close-output-port (current-output-port))
           (close-input-port (current-input-port))
           (close-output-port (current-error-port))))
        (thread-wait (server-control))))
    
    (define/private (server-control)
      (let ([listener (ssl-listen control-port (random 123456789) #t control-host control-protocol)])
        (ssl-load-certificate-chain! listener control-certificate default-locale-encoding)
        (ssl-load-private-key! listener control-key #t #f default-locale-encoding)
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
      (let ([hash-pass (read-line input-port)])
        (if (and (or ((car bad-admin-auth). < . max-admin-passwd-attempts)
                     (> ((current-seconds). - .(cdr bad-admin-auth))
                        bad-admin-auth-sleep-sec))
                 (string=? hash-pass (crypt-string (get-shadow-passwd control-admin)
                                                   (string-append "$1$" control-admin))))
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
      (send server clear-users-table)
      (load-users server (ftp-srv-params-users-file (hash-ref ftp-servers-params id)))
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
      (with-handlers ([any/c (λ(e) 
                               (when-drdebug (displayln e))
                               (print-error "Please check your ProRFTPd-config file."))])
        (call-with-input-file config-file
          (λ (in)
            (let ([conf (read in)])
              (when (eq? (car conf) 'ftp-server-config)
                (for-each 
                 (λ (param)
                   (match param
                     [`(server ,srv-id . ,srv-params)
                      (with-handlers ([any/c (λ(e) 
                                               (when-drdebug (displayln e))
                                               (print-error "Please check your ProRFTPd-config file."))])
                        (let ([id                     #f]
                              [welcome-message        server-name&version]
                              [srv-host               #f]
                              [srv-port               21]
                              [ssl-protocol           #f]
                              [ssl-key                "../certs/server-1.pem"]
                              [ssl-certificate        "../certs/server-1.pem"]
                              [max-allow-wait         25]
                              [port-timeout           15]
                              [pasv-timeout           15]
                              [data-timeout           15]
                              [session-timeout        120]
                              [max-clients-per-IP     5]
                              ;====================
                              [bad-auth-sleep-sec     60]
                              [max-auth-attempts      5]
                              [passwd-sleep-sec       0]
                              ;====================
                              [hide-dotfiles?         #f]
                              [text-user&group-names? #t]
                              [hide-ids?              #f]
                              [pasv-enable?           #t]
                              [port-enable?           #t]
                              [read-only?             #f]
                              [disable-ftp-commands   null]
                              ;====================
                              [allow-foreign-address  #f]
                              ;====================
                              [passive-host&ports     (ftp:make-passive-host&ports "127.0.0.1" 40000 40999)]
                              ;====================
                              [default-root-dir       "../ftp-dir"]
                              ;====================
                              [log-file               (format-file-name "../logs/prorftpd.log")]
                              [users-file             "../conf/prorftpd.users"])
                          (if (or (symbol? srv-id)
                                  (number? srv-id))
                              (set! id srv-id)
                              (error 'id))
                          (for-each 
                           (λ (param)
                             (match param
                               [`(welcome-message ,msg)
                                (set! welcome-message (format-welcome-msg msg))]
                               [`(host&port ,host ,port)
                                (set! srv-host (and (ftp:host-string? host) host))
                                (set! srv-port (and (ftp:port-number? port) port))]
                               [`(ssl . ,params)
                                (for-each 
                                 (λ (param)
                                   (match param
                                     [`(protocol ,proto)
                                      (set! ssl-protocol (and (ftp:ssl-protocol? proto) proto))]
                                     [`(key ,key)
                                      (set! ssl-key key)]
                                     [`(certificate ,cert)
                                      (set! ssl-certificate cert)]))
                                 params)]
                               [`(passive-host&ports ,host ,from ,to)
                                (set! passive-host&ports (ftp:make-passive-host&ports host from to))]
                               [`(max-allow-wait ,sec)
                                (set! max-allow-wait sec)]
                               [`(port-timeout ,sec)
                                (set! port-timeout sec)]
                               [`(pasv-timeout ,sec)
                                (set! pasv-timeout sec)]
                               [`(data-timeout ,sec)
                                (set! data-timeout sec)]
                               [`(session-timeout ,sec)
                                (set! session-timeout sec)]
                               [`(max-clients-per-IP ,num)
                                (set! max-clients-per-IP num)]
                               [`(bad-auth-sleep-sec ,sec)
                                (set! bad-auth-sleep-sec sec)]
                               [`(max-auth-attempts ,count)
                                (set! max-auth-attempts count)]
                               [`(passwd-sleep-sec ,sec)
                                (set! passwd-sleep-sec sec)]
                               [`(hide-dotfiles? ,flag)
                                (set! hide-dotfiles? flag)]
                               [`(text-user&group-names? ,flag)
                                (set! text-user&group-names? flag)]
                               [`(hide-ids? ,hide?)
                                (set! hide-ids? hide?)]
                               [`(pasv-enable? ,flag)
                                (set! pasv-enable? flag)]
                               [`(port-enable? ,flag)
                                (set! port-enable? flag)]
                               [`(read-only? ,flag)
                                (set! read-only? flag)]
                               [`(disable-ftp-commands ,cmds)
                                (set! disable-ftp-commands cmds)]
                               [`(allow-foreign-address ,flag)
                                (set! allow-foreign-address flag)]
                               [`(default-root-dir ,dir)
                                (set! default-root-dir dir)]
                               [`(log-file ,file)
                                (set! log-file (format-file-name file))]
                               [`(users-file ,file)
                                (set! users-file file)]))
                           srv-params)
                          (hash-set! ftp-servers-params id (ftp-srv-params welcome-message
                                                                           srv-host
                                                                           srv-port
                                                                           ssl-protocol
                                                                           ssl-key
                                                                           ssl-certificate
                                                                           max-allow-wait
                                                                           port-timeout
                                                                           pasv-timeout
                                                                           data-timeout
                                                                           session-timeout
                                                                           max-clients-per-IP
                                                                           bad-auth-sleep-sec
                                                                           max-auth-attempts 
                                                                           passwd-sleep-sec 
                                                                           hide-dotfiles?
                                                                           text-user&group-names?
                                                                           hide-ids?
                                                                           pasv-enable?
                                                                           port-enable?
                                                                           read-only?
                                                                           disable-ftp-commands
                                                                           allow-foreign-address
                                                                           passive-host&ports 
                                                                           default-root-dir
                                                                           log-file
                                                                           users-file))))]
                     [`(control-server . ,params)
                      (for-each 
                       (λ (param)
                         (with-handlers ([any/c (λ(e) 
                                                  (when-drdebug (displayln e))
                                                  (print-error "Please check your ProRFTPd-config file."))])
                           (match param
                             [`(host&port ,host ,port)
                              (set! control-host (and (ftp:host-string? host) host))
                              (set! control-port (and (ftp:port-number? port) port))]
                             [`(ssl . ,params)
                              (for-each 
                               (λ (param)
                                 (match param
                                   [`(protocol ,proto)
                                    (set! control-protocol (and (ftp:ssl-protocol? proto) proto))]
                                   [`(key ,key)
                                    (set! control-key key)]
                                   [`(certificate ,cert)
                                    (set! control-certificate cert)]))
                               params)]
                             [`(bad-admin-auth-sleep-sec ,sec)
                              (set! bad-admin-auth-sleep-sec sec)]
                             [`(max-admin-passwd-attempts ,num)
                              (set! max-admin-passwd-attempts num)])))
                       params)]
                     [`(default-locale-encoding ,encoding)
                      (set! default-locale-encoding encoding)]))
                 (cdr conf))))))))
    
    (define/private (load-users server users-file)
      (with-handlers ([any/c (λ(e) 
                               (when-drdebug (displayln e))
                               (print-error "Please check your users-config file."))])
        (call-with-input-file users-file
          (λ (in)
            (let ([conf (read in)])
              (when (eq? (car conf) 'ftp-server-users)
                (for-each (λ (user)
                            (send server useradd
                                  (car user) (second user) (third user) (fourth user) (fifth user) (sixth user)))
                          (cdr conf))))))))
    
    (define-syntax-rule (print-error msg v ...)
      (displayln (format (string-append "ProRFTPd: " msg) v ...))) 
    
    (define/private (format-welcome-msg msg)
      (regexp-replace* #rx"%[Vv]|%[Cc]"
                       msg 
                       (λ (prefix)
                         (case (string->symbol (string-upcase prefix))
                           [(%V) server-name&version]
                           [(%C) copyright]
                           [else prefix]))))))

;-----------------------------------
;              BEGIN
;-----------------------------------
(date-display-format 'iso-8601)
(unless-drdebug
 (let ([run-dir-path (path-only (find-system-path 'run-file))])
   (when run-dir-path
     (current-directory run-dir-path))))
(send (new racket-ftp-server% [read-cmd-line? #t]) main)