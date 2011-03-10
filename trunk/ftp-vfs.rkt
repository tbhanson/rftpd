#|
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

;(require (file "common.rkt"))
(provide (all-defined-out))

(struct ftp-user (full-name login pass group home-dirs root-dir))

(define ftp-vfs-file-extension #"ftp-racket-file")
(define ftp-vfs-dir-extension #"ftp-racket-directory")
(define ftp-vfs-file-spath ".ftp-racket-file")
(define ftp-vfs-dir-spath "/.ftp-racket-directory")

(define (real-path->ftp-path real-path root-dir [drop-tail-elem 0])
  (simplify-ftp-path (substring real-path (string-length root-dir)) drop-tail-elem))

(define (ftp-file-or-dir-full-info sys-file)
  (call-with-input-file sys-file
    (λ (in)
      (vector (integer-bytes->integer (read-bytes 2 in) #f) ; sysbytes
              (read-line in)                                ; owner
              (read-line in)))))                            ; group

(define (ftp-file-or-dir-sysbytes sys-file)
  (call-with-input-file sys-file
    (λ (in)
      (integer-bytes->integer (read-bytes 2 in) #f))))

(define (ftp-file-or-dir-sysbytes/owner sys-file)
  (call-with-input-file sys-file
    (λ (in)
      (vector (integer-bytes->integer (read-bytes 2 in) #f)
              (read-line in)))))

(define (ftp-mkdir spath [owner "racket"][group "racket"][permissions #b111101101])
  (unless (directory-exists? spath)
    (make-directory spath))
  (ftp-mksys-file (string-append spath ftp-vfs-dir-spath)
                  owner group permissions))

(define (ftp-mksys-file sys-file [owner "racket"][group "racket"][permissions #b110100100])
  (call-with-output-file sys-file
    (λ (out)
      [write-bytes (integer->integer-bytes permissions 2 #f) out]
      [display owner out][newline out]
      [display group out][newline out])
    #:exists 'truncate))

(define (ftp-dir-exists? spath)
  (and (directory-exists? spath)
       (file-exists? (string-append spath ftp-vfs-dir-spath))))

(define (ftp-file-exists? spath)
  (and (file-exists? spath)
       (file-exists? (string-append spath ftp-vfs-file-spath))))

(define (ftp-file-name-safe? spath)
  (not (and (filename-extension spath)
            (or (bytes=? (filename-extension spath) ftp-vfs-file-extension)
                (bytes=? (filename-extension spath) ftp-vfs-dir-extension)))))

(define (build-ftp-spath current-dir . spaths)
  (let ([spath (apply string-append spaths)])
    (if (and (pair? spaths)
             (memq (string-ref spath 0) '(#\\ #\/)))
        (path->string (simplify-path spath #f))
        (string-append current-dir 
                       (path->string (simplify-path (string-append "/" spath) #f))))))

(define (simplify-ftp-path ftp-path [drop-tail-elem 0])
  (with-handlers ([any/c (λ (e) "/")])
    (let ([path-lst (drop-right (filter (λ (s) (not (or (string=? s "") (string=? s ".."))))
                                        (regexp-split #rx"[/\\\\]+" (simplify-path ftp-path #f)))
                                drop-tail-elem)])
      (if (null? path-lst)
          "/"
          (if (memq (string-ref ftp-path 0) '(#\\ #\/))
              (foldr (λ (a b) (string-append "/" a b)) "" path-lst)
              (string-append (car path-lst) 
                             (foldr (λ (a b) (string-append "/" a b)) "" (cdr path-lst))))))))

(define (member-ftp-group? groups user group)
  (or (string=? group (ftp-user-group user))
      (and (hash-ref groups group #f)
           (hash-ref (hash-ref groups group) (ftp-user-login user) #f))))

(define (ftp-allow-read? full-ftp-sys-file-spath user groups)
  (let ([info (ftp-file-or-dir-full-info full-ftp-sys-file-spath)])
    (or (cond
          ((string=? (vector-ref info 1) (ftp-user-login user))
           (bitwise-bit-set? (vector-ref info 0) 8))
          ((member-ftp-group? groups user (vector-ref info 2))
           (bitwise-bit-set? (vector-ref info 0) 5))
          (else
           (bitwise-bit-set? (vector-ref info 0) 2)))
        (member-ftp-group? groups user "root"))))

(define (ftp-allow-write? full-ftp-sys-file-spath user groups)
  (let ([info (ftp-file-or-dir-full-info full-ftp-sys-file-spath)])
    (or (cond
          ((string=? (vector-ref info 1) (ftp-user-login user))
           (bitwise-bit-set? (vector-ref info 0) 7))
          ((member-ftp-group? groups user (vector-ref info 2))
           (bitwise-bit-set? (vector-ref info 0) 4))
          (else
           (bitwise-bit-set? (vector-ref info 0) 1)))
        (member-ftp-group? groups user "root"))))

(define (ftp-allow-execute? full-ftp-sys-file-spath user groups)
  (let ([info (ftp-file-or-dir-full-info full-ftp-sys-file-spath)])
    (or (cond
          ((string=? (vector-ref info 1) (ftp-user-login user))
           (bitwise-bit-set? (vector-ref info 0) 6))
          ((member-ftp-group? groups user (vector-ref info 2))
           (bitwise-bit-set? (vector-ref info 0) 3))
          (else
           (bitwise-bit-set? (vector-ref info 0) 0)))
        (member-ftp-group? groups user "root"))))

(define (ftp-access-allow? ftp-root-dir 
                           ftp-full-spath 
                           user 
                           groups
                           [drop-tail-elem 0])
  (letrec ([test
            (λ(curr dirlist)
              (if (null? dirlist)
                  #t
                  (let ([dir (string-append curr "/" (car dirlist))])
                    (and (ftp-allow-execute? (string-append dir ftp-vfs-dir-spath) user groups)
                         (test dir (cdr dirlist))))))])
    (let ([dirs (filter (λ (s) (not (string=? s "")))
                        (regexp-split #rx"[/\\\\]+" (simplify-path ftp-full-spath #f)))])
      (and (ftp-allow-execute? (string-append ftp-root-dir ftp-vfs-dir-spath) user groups)
           (or (null? dirs)
               (test ftp-root-dir (drop-right dirs drop-tail-elem)))))))

(define (ftp-dir-allow-read? spath user groups)
  (ftp-allow-read? (string-append spath ftp-vfs-dir-spath) user groups))

(define (ftp-dir-allow-write? spath user groups)
  (ftp-allow-write? (string-append spath ftp-vfs-dir-spath) user groups))

(define (ftp-dir-allow-execute? spath user groups)
  (ftp-allow-execute? (string-append spath ftp-vfs-dir-spath) user groups))

(define (ftp-file-allow-read? spath user groups)
  (ftp-allow-read? (string-append spath ftp-vfs-file-spath) user groups))

(define (ftp-file-allow-write? spath user groups)
  (ftp-allow-write? (string-append spath ftp-vfs-file-spath) user groups))