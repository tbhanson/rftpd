#|

Racket FTP Server v1.0.11
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
    
    (init-field [name&version "Racket FTP Server v1.0.11"]
                [copyright "Copyright (c) 2010-2011 Mikhail Mosienko <cnet@land.ru>"]
                
                [server-ip4-host        "127.0.0.1"]
                [server-ip4-port        21]
                [server-ip4-encryption  #f]
                [server-ip4-certificate "certs/ip4.pem"]
                
                [server-ip6-host        "::1"]
                [server-ip6-port        21]
                [server-ip6-encryption  #f]
                [server-ip6-certificate "certs/ip6.pem"]
                
                [server-max-allow-wait 50]
                
                [passive-ip4-ports '(40000 . 40599)]
                [passive-ip6-ports '(40000 . 40599)]
                
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
                        [server-ip4-host server-ip4-host]
                        [server-ip4-port server-ip4-port]
                        [server-ip4-encryption server-ip4-encryption]
                        [server-ip4-certificate server-ip4-certificate]
                        [server-ip6-host server-ip6-host]
                        [server-ip6-port server-ip6-port]
                        [server-ip6-encryption server-ip6-encryption]
                        [server-ip6-certificate server-ip6-certificate]
                        [max-allow-wait server-max-allow-wait]
                        [passive-ip4-ports-from (car passive-ip4-ports)]
                        [passive-ip4-ports-to (cdr passive-ip4-ports)]
                        [passive-ip6-ports-from (car passive-ip6-ports)]
                        [passive-ip6-ports-to (cdr passive-ip6-ports)]
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
            (ssl-load-certificate-chain! listener control-cert-file)
            (ssl-load-private-key! listener control-cert-file)
            (letrec ([main-loop (λ ()
                                  (handle-client-request listener)
                                  (main-loop))])
              (thread main-loop))))))
    
    (define/private (handle-client-request listener)
      (let ([cust (make-custodian)])
        (parameterize ([current-custodian cust])
          (let-values ([(in out) (ssl-accept listener)])
            (thread (λ ()
                      (with-handlers ([any/c #|displayln|# void])
                        (eval-cmd in out))
                      (close-output-port out)
                      (custodian-shutdown-all cust)))))))
    
    (define/private (eval-cmd input-port output-port)
      (let ([pass (read-line input-port)])
        (when (string=? pass control-passwd)
          (displayln "Ok" output-port)
          (flush-output output-port)
          (let next ([cmd (read input-port)])
            (cond
              ((eof-object? cmd))
              ((eq? cmd '%exit)
               (stop)
               (close-output-port log-out)
               (displayln "Ok" output-port) (flush-output output-port)
               (exit))
              ((eq? cmd '%run)
               (run)
               (displayln "Ok" output-port) (flush-output output-port)
               (next (read input-port)))
              ((eq? cmd '%stop)
               (flush-output log-out)
               (stop)
               (displayln "Ok" output-port) (flush-output output-port)
               (next (read input-port)))
              ((eq? cmd '%restart)
               (flush-output log-out)
               (stop)
               (run)
               (displayln "Ok" output-port) (flush-output output-port)
               (next (read input-port)))
              (else
               (fprintf output-port "Command '~a' not implemented.\n" cmd) (flush-output output-port)
               (next (read input-port))))))))
    
    (define/private (run)
      (send server run)
      (displayln "Server running!"))
    
    (define/private (stop)
      (send server stop)
      (displayln "Server stopped!"))
    
    (define/private (load-config)
      (call-with-input-file config-file
        (λ (in)
          (let ([conf (read in)])
            (when (eq? (car conf) 'ftp-server-config)
              (for-each (λ (param)
                          (case (car param)
                            ((server-ip4)
                             (with-handlers ([any/c void])
                               (set! server-ip4-host (second param))
                               (set! server-ip4-port (third param))
                               (set! server-ip4-encryption (fourth param))))
                            ((server-ip4-certificate)
                             (set! server-ip4-certificate (second param)))
                            ((server-ip6)
                             (with-handlers ([any/c void])
                               (set! server-ip6-host (second param))
                               (set! server-ip6-port (third param))
                               (set! server-ip6-encryption (fourth param))))
                            ((server-ip6-certificate)
                             (set! server-ip6-certificate (second param)))
                            ((max-allow-wait)
                             (set! server-max-allow-wait (second param)))
                            ((passive-ip4-ports)
                             (set! passive-ip4-ports (cons (second param) (third param))))
                            ((passive-ip6-ports)
                             (set! passive-ip6-ports (cons (second param) (third param))))
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

