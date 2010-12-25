#|

Racket FTP Server Control Interface v1.0.5
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

#lang racket/base

(require racket/cmdline
         racket/list
         (file "lib-ssl.rkt")
         (for-syntax racket/base))

(define-syntax (any/exc stx) #'void)

(define name&version "Racket FTP Server Control Interface v1.0.5")
(define copyright    "Copyright (c) 2010-2011 Mikhail Mosienko <cnet@land.ru>")
(define help-msg     "Type 'help' or '?' for help.")

(define config-file      (make-parameter "conf/ftp.config"))
(define control-host     (make-parameter "127.0.0.1"))
(define control-port     (make-parameter 40600))
(define control-passwd   (make-parameter "12345"))

(define interactive-mode (make-parameter #t))

(define shutdown? (make-parameter #f))
(define run?      (make-parameter #f))
(define stop?     (make-parameter #f))
(define restart?  (make-parameter #f))

(printf "~a\n~a\n\n" name&version copyright)

(command-line
 #:program "control"
 #:once-any
 [("-r" "--run")     "Run server"      (run? #t)]
 [("-s" "--stop")    "Stop server"     (stop? #t)]
 [("-t" "--restart") "Restart server"  (restart? #t)]
 [("-e" "--exit")    "Shutdown server" (shutdown? #t)])

(let ([cust (make-custodian)])
  (parameterize ([current-custodian cust])
    (with-handlers ([any/exc void])
      (call-with-input-file (config-file)
        (λ (in)
          (let ([conf (read in)])
            (for-each (λ (param)
                        (case (car param)
                          ((control-server)
                           (with-handlers ([any/exc void])
                             (control-host (second param))
                             (control-port (third param))))
                          ((control-passwd)
                           (control-passwd (second param)))))
                      (cdr conf)))))
      (let-values ([(in out) (ssl-connect (control-host) (control-port) 'sslv3)])
        (displayln (control-passwd) out)(flush-output out)
        (when (string=? (read-line in) "Ok")
          (cond
            ((run?)
             (displayln "%run" out)
             (flush-output out)
             (displayln (read-line in)))
            ((stop?)
             (displayln "%stop" out)
             (flush-output out)
             (displayln (read-line in)))
            ((restart?)
             (displayln "%restart" out)
             (flush-output out)
             (displayln (read-line in)))
            ((shutdown?)
             (displayln "%exit" out)
             (flush-output out)
             (displayln (read-line in)))
            (else
             (when (interactive-mode)
               (displayln help-msg) (newline)
               (let loop ()
                 (display "#> ")
                 (let ([cmd (read)])
                   (case cmd
                     ((help ?)
                      (displayln "%run     - run server.")
                      (displayln "%stop    - stop server.")
                      (displayln "%restart - restart server.")
                      (displayln "%exit    - shutdown server.")
                      (loop))
                     (else
                      (displayln cmd out)
                      (flush-output out)
                      (displayln (read-line in))
                      (unless (eq? cmd '%exit)
                        (loop))))))))))))
    (custodian-shutdown-all cust)))
