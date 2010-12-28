#|

Racket FTP Server v1.0.12
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

(require (file "lib-ssl.rkt")
         (prefix-in ftp: (file "lib-rftpd.rkt")))

(define racket-ftp-server%
  (class object%
    (super-new)
    
    (init-field [name&version "Racket FTP Server v1.0.12"]
                [copyright "Copyright (c) 2010-2011 Mikhail Mosienko <cnet@land.ru>"]
                
                [server-1-host        "127.0.0.1"]
                [server-1-port        21]
                [server-1-encryption  #f]
                [server-1-certificate "certs/server-1.pem"]
                
                [server-2-host        "::1"]
                [server-2-port        21]
                [server-2-encryption  #f]
                [server-2-certificate "certs/server-2.pem"]
                
                [server-max-allow-wait 50]
                
                [passive-1-ports '(40000 . 40599)]
                [passive-2-ports '(40000 . 40599)]
                
                [control-passwd "12345"]
                [control-host "127.0.0.1"]
                [control-port 40600]
                
                [default-locale-encoding "UTF-8"]
                [default-root-dir        "ftp-dir"]
                
                [log-file "logs/ftp.log"]
                [control-cert-file "certs/control.pem"]
                [config-file "conf/ftp.config"]
                [users-file "conf/ftp.users"])
    
    (define log-out #f)
    (define server #f)
    ;;
    ;; ---------- Public Methods ----------
    ;;
    (define/public (start)
      (load-config)
      (unless log-out
        (set! log-out (open-output-file log-file #:exists 'append)))
      (set! server (new ftp:ftp-server% 
                        [welcome-message name&version]
                        [server-1-host server-1-host]
                        [server-1-port server-1-port]
                        [server-1-encryption server-1-encryption]
                        [server-1-certificate server-1-certificate]
                        [server-2-host server-2-host]
                        [server-2-port server-2-port]
                        [server-2-encryption server-2-encryption]
                        [server-2-certificate server-2-certificate]
                        [max-allow-wait server-max-allow-wait]
                        [passive-1-ports-from (car passive-1-ports)]
                        [passive-1-ports-to (cdr passive-1-ports)]
                        [passive-2-ports-from (car passive-2-ports)]
                        [passive-2-ports-to (cdr passive-2-ports)]
                        [default-root-dir default-root-dir]
                        [default-locale-encoding default-locale-encoding]
                        [log-output-port log-out]))
      (load-users)
      (displayln name&version)
      (displayln copyright) (newline)
      (run)
      (server-control)
      (let loop ()
        (sleep 60)
        (loop)))
    ;;
    ;; ---------- Private Methods ----------
    ;;
    (define/private (server-control)
      (let ([cust (make-custodian)])
        (parameterize ([current-custodian cust])
          (let ([listener (ssl-listen control-port (random 123456789) #t control-host 'sslv3)])
            (ssl-load-certificate-chain! listener control-cert-file default-locale-encoding)
            (ssl-load-private-key! listener control-cert-file #t #f default-locale-encoding)
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
        (when (string=? pass control-passwd)
          (let ([response (λ msg
                            (apply fprintf output-port msg)
                            (newline output-port)
                            (flush-output output-port))])
            (response "Ok")
            (let next ([cmd (read input-port)])
              (cond
                ((eof-object? cmd))
                ((eq? cmd '%bye)
                 (response "Ok"))
                ((eq? cmd '%exit)
                 (stop)
                 (close-output-port log-out)
                 (response "Ok")
                 (exit))
                ((eq? cmd '%run)
                 (run)
                 (response "Ok")
                 (next (read input-port)))
                ((eq? cmd '%stop)
                 (flush-output log-out)
                 (stop)
                 (response "Ok")
                 (next (read input-port)))
                ((eq? cmd '%restart)
                 (flush-output log-out)
                 (stop)
                 (run)
                 (response "Ok")
                 (next (read input-port)))
                (else
                 (response "Command '~a' not implemented.\n" cmd)
                 (next (read input-port)))))))))
    
    (define/private (run)
      (send server run)
      (displayln (format "Server ~a!" (send server status))))
    
    (define/private (stop)
      (send server stop)
      (displayln (format "Server ~a!" (send server status))))
    
    (define/private (load-config)
      (call-with-input-file config-file
        (λ (in)
          (let ([conf (read in)])
            (when (eq? (car conf) 'ftp-server-config)
              (for-each (λ (param)
                          (case (car param)
                            ((server-1)
                             (with-handlers ([any/c void])
                               (set! server-1-host (second param))
                               (set! server-1-port (third param))
                               (set! server-1-encryption (fourth param))))
                            ((server-1-certificate)
                             (set! server-1-certificate (second param)))
                            ((server-2)
                             (with-handlers ([any/c void])
                               (set! server-2-host (second param))
                               (set! server-2-port (third param))
                               (set! server-2-encryption (fourth param))))
                            ((server-2-certificate)
                             (set! server-2-certificate (second param)))
                            ((max-allow-wait)
                             (set! server-max-allow-wait (second param)))
                            ((passive-1-ports)
                             (set! passive-1-ports (cons (second param) (third param))))
                            ((passive-2-ports)
                             (set! passive-2-ports (cons (second param) (third param))))
                            ((default-locale-encoding)
                             (set! default-locale-encoding (second param)))
                            ((default-root-dir)
                             (set! default-root-dir (second param)))
                            ((log-file)
                             (when log-out (close-output-port log-out))
                             (set! log-out (open-output-file (second param) #:exists 'append)))
                            ((control-server)
                             (with-handlers ([any/c void])
                               (set! control-host (second param))
                               (set! control-port (third param))))
                            ((control-passwd)
                             (set! control-passwd (second param)))
                            ((control-certificate)
                             (set! control-cert-file (second param)))))
                        (cdr conf)))))))
    
    (define/private (load-users)
      (call-with-input-file users-file
        (λ (in)
          (let ([conf (read in)])
            (when (eq? (car conf) 'ftp-server-users)
              (for-each (λ (user)
                          (send server add-ftp-user
                                (car user) (second user) (third user) (fourth user) (fifth user) (sixth user)))
                        (cdr conf)))))))))
;-----------------------------------
;              BEGIN
;-----------------------------------
(send (new racket-ftp-server%) start)

