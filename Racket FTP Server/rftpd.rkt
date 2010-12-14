#|

Racket FTP Server v1.0.8
----------------------------------------------------------------------

Summary:
This file is part of Racket FTP Server.

License:
Copyright (c) 2010-2011 Mikhail Mosienko <cnet@land.ru>
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

(require (prefix-in ftp: (file "lib-rftpd.rkt"))
         racket/class)

(define name&version "Racket FTP Server v1.0.8")
(define copyright "Copyright (c) 2010-2011 Mikhail Mosienko <cnet@land.ru>")

(define server-host "127.0.0.1")
(define server-port 21)
(define server-max-allow-wait 50)

(define control-passwd "12345")
(define control-host "127.0.0.1")
(define control-port 40600)

(define main-server-custodian #f)
(define control-custodian #f)
(define log-out #f)

(define server (new ftp:ftp-server%))

(define (server-control [host "127.0.0.1"] [port 40600] [max-allow-wait 5])
  (let ([cust (make-custodian)])
    (parameterize ([current-custodian cust])
      (letrec ([listener (tcp-listen port max-allow-wait #t host)]
               [main-loop (λ ()
                            (handle-client-request listener)
                            (main-loop))])
        (thread main-loop)))
    (set! control-custodian cust)))

(define (handle-client-request listener)
  (let ([cust (make-custodian)])
    (parameterize ([current-custodian cust])
      (let-values ([(in out) (tcp-accept listener)])
        (thread (λ ()
                  (with-handlers ([any/c #|displayln|# void])
                    (eval-cmd in out))
                  (custodian-shutdown-all cust)))))))

(define (eval-cmd input-port output-port)
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

(define (run)
  (set! main-server-custodian (send server run-server server-port server-max-allow-wait server-host))
  (displayln "Server running!"))

(define (stop)
  (custodian-shutdown-all main-server-custodian)
  (displayln "Server stopped!"))

(define (start)
  (load-config)
  (load-users)
  (unless log-out
    (set! log-out (open-output-file "logs/ftp.log" #:exists 'append)))
  (set-field! log-output-port server log-out)
  (displayln name&version)
  (displayln copyright) (newline)
  (run)
  (server-control control-host control-port)
  (let loop ()
    (sleep 60)
    (loop)))

(define (load-config [file-name "conf/ftp.config"])
  (call-with-input-file file-name
    (λ (in)
      (let ([conf (read in)])
        (when (eq? (car conf) 'ftp-server-config)
          (for-each (λ (param)
                      (case (car param)
                        ((host)
                         (set! server-host (second param)))
                        ((port)
                         (set! server-port (second param)))
                        ((max-allow-wait)
                         (set! server-max-allow-wait (second param)))
                        ((passive-ports)
                         (send server set-passive-ports (second param) (third param)))
                        ((default-locale-encoding)
                         (ftp:default-locale-encoding (second param)))
                        ((default-root-dir)
                         (ftp:default-root-dir (second param)))
                        ((log-file)
                         (when log-out (close-output-port log-out))
                         (set! log-out (open-output-file (second param) #:exists 'append)))
                        ((control-host)
                         (set! control-host (second param)))
                        ((control-port)
                         (set! control-port (second param)))
                        ((control-passwd)
                         (set! control-passwd (second param)))))
                    (cdr conf)))))))

(define (load-users [file-name "conf/ftp.users"])
  (call-with-input-file file-name
    (λ (in)
      (let ([conf (read in)])
        (when (eq? (car conf) 'ftp-server-users)
          (for-each (λ (user)
                      (send server add-ftp-user
                            (car user) (second user) (third user) (fourth user) (fifth user) (sixth user)))
                    (cdr conf)))))))

(start)

