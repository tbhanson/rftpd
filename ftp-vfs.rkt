#|

RFTPd VFS Library v1.0.2
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

(require (file "lib-string.rkt"))

(provide (except-out (all-defined-out)
                     ftp-users/login
                     ftp-users/uid
                     ftp-groups/name
                     ftp-users/uid))

(struct ftp-user (login pass uid gid root-dir home-dirs info))
(struct ftp-group (name gid users))

(define ftp-vfs-file-extension #"ftp-racket-file")
(define ftp-vfs-dir-extension #"ftp-racket-directory")
(define ftp-vfs-file-spath ".ftp-racket-file")
(define ftp-vfs-dir-spath "/.ftp-racket-directory")

(define ftp-users/login (make-hash))
(define ftp-users/uid (make-hash))

(define ftp-groups/name (make-hash))
(define ftp-groups/gid (make-hash))
(define-syntax (root-gid so) #'0)

;(define (userinfo id)
;  (if (string? id)
;      (hash-ref ftp-users/login id #f)
;      (hash-ref ftp-users/uid id #f)))

(define (userinfo/login login)
  (hash-ref ftp-users/login login #f))

(define (userinfo/uid uid)
  (hash-ref ftp-users/uid uid #f))

(define (login->uid login)
  (let ([user (hash-ref ftp-users/login login #f)])
    (and user (ftp-user-uid user))))

(define (uid->login uid)
  (let ([user (hash-ref ftp-users/uid uid #f)])
    (and user (ftp-user-login user))))

(define (groupinfo/name name)
  (hash-ref ftp-groups/name name #f))

(define (groupinfo/uid gid)
  (hash-ref ftp-groups/gid gid #f))

(define (group-name->gid name)
  (let ([group (hash-ref ftp-groups/name name #f)])
    (and group (ftp-group-gid group))))

(define (gid->group-name gid)
  (let ([group (hash-ref ftp-groups/gid gid #f)])
    (and group (ftp-group-name group))))

(define (ftp-useradd login pass uid gid [root-dir "/"] [home-dirs '("/")] [info ""])
  (let ([root-dir (delete-lrws root-dir)])
    (let ([user (ftp-user login pass uid gid root-dir home-dirs info)])
      (hash-set! ftp-users/login login user)
      (hash-set! ftp-users/uid uid user))
    (unless (ftp-dir-exists? root-dir)
      (ftp-mkdir root-dir uid gid))
    (for-each (λ (home-dir)
                (unless (ftp-dir-exists? (string-append root-dir home-dir))
                  (let ([dirs (filter (λ (s) (not (string=? s "")))
                                      (string-split '(#\\ #\/) home-dir))]
                        [curr-dir ""])
                    (unless (zero? (length dirs))
                      (for-each (λ (p)
                                  (unless (ftp-dir-exists? (string-append root-dir curr-dir "/" p))
                                    (ftp-mkdir (string-append root-dir curr-dir "/" p)
                                               uid gid))
                                  (set! curr-dir (string-append curr-dir "/" p)))
                                (drop-right dirs 1)))
                    (ftp-mkdir (string-append root-dir home-dir) uid gid))))
              home-dirs)))

(define (ftp-groupadd name gid users)
  (let ([users (ftp-group name gid (make-hash (map (λ(u) (cons u #t)) users)))])
    (hash-set! ftp-groups/name name users)
    (hash-set! ftp-groups/gid gid users)))

(define (ftp-clear-users)
  (set! ftp-users/login (make-hash))
  (set! ftp-users/uid (make-hash)))

(define (ftp-clear-groups)
  (set! ftp-groups/name (make-hash))
  (set! ftp-groups/gid (make-hash)))

(define (real-path->ftp-path real-path root-dir [drop-tail-elem 0])
  (simplify-ftp-path (substring real-path (string-length root-dir)) drop-tail-elem))

(define (ftp-file-or-dir/full-info sys-file)
  (call-with-input-file sys-file
    (λ (in)
      (vector (integer-bytes->integer (read-bytes 2 in) #f)     ; sysbytes
              (integer-bytes->integer (read-bytes 4 in) #f)     ; uid
              (integer-bytes->integer (read-bytes 4 in) #f))))) ; gid

(define (ftp-file-or-dir/sysbytes sys-file)
  (call-with-input-file sys-file
    (λ (in)
      (integer-bytes->integer (read-bytes 2 in) #f))))

(define (ftp-file-or-dir/sysbytes+uid sys-file)
  (call-with-input-file sys-file
    (λ (in)
      (vector (integer-bytes->integer (read-bytes 2 in) #f)
              (integer-bytes->integer (read-bytes 4 in) #f)))))

(define (ftp-mkdir spath [uid 0][gid root-gid][permissions #b111101101])
  (unless (directory-exists? spath)
    (make-directory spath))
  (ftp-mksysfile (string-append spath ftp-vfs-dir-spath)
                 uid gid permissions))

(define (ftp-mksysfile sysfile [uid 0][gid root-gid][permissions #b110100100])
  (call-with-output-file sysfile
    (λ (out)
      [write-bytes (integer->integer-bytes permissions 2 #f) out]
      [write-bytes (integer->integer-bytes uid 4 #f) out]
      [write-bytes (integer->integer-bytes gid 4 #f) out])
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

(define (member-ftp-group/gid? userstruct gid)
  (or (= gid (ftp-user-gid userstruct))
      (and (hash-ref ftp-groups/gid gid #f)
           (hash-ref (ftp-group-users (hash-ref ftp-groups/gid gid))
                     (ftp-user-login userstruct) #f))))

(define (member-ftp-group/name? userstruct group-name)
  (member-ftp-group/gid? userstruct (group-name->gid group-name)))

(define (ftp-vfs-obj-allow-read? full-ftp-sysfile-spath userstruct)
  (let ([info (ftp-file-or-dir/full-info full-ftp-sysfile-spath)])
    (or (member-ftp-group/gid? userstruct root-gid)
        (cond
          ((= (vector-ref info 1) (ftp-user-uid userstruct))
           (bitwise-bit-set? (vector-ref info 0) 8))
          ((member-ftp-group/gid? userstruct (vector-ref info 2))
           (bitwise-bit-set? (vector-ref info 0) 5))
          (else
           (bitwise-bit-set? (vector-ref info 0) 2))))))

(define (ftp-vfs-obj-allow-write? full-ftp-sysfile-spath userstruct)
  (let ([info (ftp-file-or-dir/full-info full-ftp-sysfile-spath)])
    (or (member-ftp-group/gid? userstruct root-gid)
        (cond
          ((= (vector-ref info 1) (ftp-user-uid userstruct))
           (bitwise-bit-set? (vector-ref info 0) 7))
          ((member-ftp-group/gid? userstruct (vector-ref info 2))
           (bitwise-bit-set? (vector-ref info 0) 4))
          (else
           (bitwise-bit-set? (vector-ref info 0) 1))))))

(define (ftp-vfs-obj-allow-execute? full-ftp-sysfile-spath userstruct)
  (let ([info (ftp-file-or-dir/full-info full-ftp-sysfile-spath)])
    (or (member-ftp-group/gid? userstruct root-gid)
        (cond
          ((= (vector-ref info 1) (ftp-user-uid userstruct))
           (bitwise-bit-set? (vector-ref info 0) 6))
          ((member-ftp-group/gid? userstruct (vector-ref info 2))
           (bitwise-bit-set? (vector-ref info 0) 3))
          (else
           (bitwise-bit-set? (vector-ref info 0) 0))))))

(define (ftp-vfs-obj-access-allow? ftp-root-dir 
                                   ftp-full-spath 
                                   userstruct
                                   [drop-tail-elem 0])
  (letrec ([test
            (λ(curr dirlist)
              (if (null? dirlist)
                  #t
                  (let ([dir (string-append curr "/" (car dirlist))])
                    (and (ftp-vfs-obj-allow-execute? (string-append dir ftp-vfs-dir-spath) userstruct)
                         (test dir (cdr dirlist))))))])
    (let ([dirs (filter (λ (s) (not (string=? s "")))
                        (regexp-split #rx"[/\\\\]+" (simplify-path ftp-full-spath #f)))])
      (and (ftp-vfs-obj-allow-execute? (string-append ftp-root-dir ftp-vfs-dir-spath) userstruct)
           (or (null? dirs)
               (test ftp-root-dir (drop-right dirs drop-tail-elem)))))))

(define (ftp-dir-allow-read? spath userstruct)
  (ftp-vfs-obj-allow-read? (string-append spath ftp-vfs-dir-spath) userstruct))

(define (ftp-dir-allow-write? spath userstruct)
  (ftp-vfs-obj-allow-write? (string-append spath ftp-vfs-dir-spath) userstruct))

(define (ftp-dir-allow-execute? spath userstruct)
  (ftp-vfs-obj-allow-execute? (string-append spath ftp-vfs-dir-spath) userstruct))

(define (ftp-file-allow-read? spath userstruct)
  (ftp-vfs-obj-allow-read? (string-append spath ftp-vfs-file-spath) userstruct))

(define (ftp-file-allow-write? spath userstruct)
  (ftp-vfs-obj-allow-write? (string-append spath ftp-vfs-file-spath) userstruct))