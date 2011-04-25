#|

RFTPd Package Builder v1.5
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

(require racket/file
         compiler/embed
         compiler/distribute)

(define package-dir "rftpd-package")
(define compiled-dir "compiled")
(define temp-dir (find-system-path 'temp-dir))
(define dest-exe (build-path temp-dir 
                             (if (eq? (system-type) 'windows) 
                                 "rftpd.exe"
                                 "rftpd")))
(define certs-src "certs")
(define certs-dest (build-path package-dir "certs"))
(define conf-src (if (eq? (system-type) 'windows)
                     "conf"
                     "conf-unix"))
(define conf-dest (build-path package-dir "conf"))
(define license-src "LICENSE")
(define license-dest (build-path package-dir "LICENSE"))
(define copyright-src "COPYRIGHT")
(define copyright-dest (build-path package-dir "COPYRIGHT"))
(define flags "flags.rkt")
(define flags-temp "flags.copy")

(display "Please wait for final assembly of the package")
(define thd (letrec ([loop (λ() 
                             (sleep 1/2) 
                             (write-char #\.)
                             (flush-output)
                             (loop))])
              (thread loop)))

(with-handlers ([void (λ (e) 
                        (kill-thread thd)
                        (printf "\nError:\n~a\n" e))])
  (when (file-exists? dest-exe)
    (delete-file dest-exe))
  
  (when (directory-exists? package-dir)
    (delete-directory/files package-dir))
  
  (when (directory-exists? compiled-dir)
    (delete-directory/files compiled-dir))
  
  (make-directory package-dir)
  (copy-directory/files conf-src conf-dest)
  (copy-directory/files certs-src certs-dest)
  (copy-file license-src license-dest)
  (copy-file copyright-src copyright-dest)
  (rename-file-or-directory flags flags-temp)
  
  (display-lines-to-file
   '("#lang racket/base"
     "(require (for-syntax racket/base))"
     "(provide (for-syntax create-executable?))"
     "(define-for-syntax create-executable? #t)")
   flags)
  
  (parameterize ([current-namespace (make-base-namespace)])
    (create-embedding-executable 
     dest-exe
     #:modules '((#f (file "rftpd.rkt")))
     #:cmdline '("-U" "--")
     #:literal-expression
     (compile '(namespace-require ''rftpd)))
    (assemble-distribution package-dir (list dest-exe)))
  
  (delete-file dest-exe)
  (delete-file flags)
  (rename-file-or-directory flags-temp flags)
  (kill-thread thd))