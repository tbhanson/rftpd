#|

RFTPd VFS Library v1.0.6
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
                     (struct-out ftp-users&groups)
                     ftp-users/uid
                     ftp-groups/gid
                     ftp-vfs-obj-allow-read?
                     ftp-vfs-obj-allow-write?
                     ftp-vfs-obj-allow-execute?
                     file-path->sysfile-path
                     dir-path->sysfile-path
                     ftp-vfs-file-extension
                     ftp-vfs-dir-extension
                     ftp-vfs-file-spath
                     ftp-vfs-dir-spath))

(struct ftp-user (login pass uid gid ftp-perm root-dir home-dirs info))
(struct ftp-group (name gid users))

(struct ftp-users&groups (users/login users/uid groups/name groups/gid) #:mutable)
(struct ftp-client (ip [userStruct #:mutable] users&groups))

(struct ftp-permissions (l? r? a? c? m? f? d?))

(define ftp-vfs-file-extension #"ftp-racket-file")
(define ftp-vfs-dir-extension #"ftp-racket-directory")
(define ftp-vfs-file-spath ".ftp-racket-file")
(define ftp-vfs-dir-spath "/.ftp-racket-directory")

(define-syntax (root-gid so) #'0)

(define (make-users&groups)
  (ftp-users&groups (make-hash) (make-hash) (make-hash) (make-hash)))

(define-syntax-rule (ftp-users/uid clientStruct)
  (ftp-users&groups-users/uid (ftp-client-users&groups clientStruct)))

(define-syntax-rule (ftp-groups/gid clientStruct)
  (ftp-users&groups-groups/gid (ftp-client-users&groups clientStruct)))

(define-syntax-rule (ftp-user/uid clientStruct)
  (ftp-user-uid (ftp-client-userStruct clientStruct)))

(define-syntax-rule (ftp-user/ftp-perm clientStruct)
  (ftp-user-ftp-perm (ftp-client-userStruct clientStruct)))

(define-syntax-rule (file-path->sysfile-path spath)
  (string-append spath ftp-vfs-file-spath))

(define-syntax-rule (dir-path->sysfile-path spath)
  (string-append spath ftp-vfs-dir-spath))

;=============================

(define (clear-users-info users&groups)
  (set-ftp-users&groups-users/login! users&groups (make-hash))
  (set-ftp-users&groups-users/uid! users&groups (make-hash)))

(define (userinfo/login users&groups login)
  (hash-ref (ftp-users&groups-users/login users&groups) login #f))

(define (userinfo/uid users&groups uid)
  (hash-ref (ftp-users&groups-users/uid users&groups) uid #f))

(define (login->uid users&groups login)
  (let ([user (hash-ref (ftp-users&groups-users/login users&groups) login #f)])
    (and user (ftp-user-uid user))))

(define (uid->login users&groups uid)
  (let ([user (hash-ref (ftp-users&groups-users/uid users&groups) uid #f)])
    (and user (ftp-user-login user))))

(define (clear-groups-info users&groups)
  (set-ftp-users&groups-groups/name! users&groups (make-hash))
  (set-ftp-users&groups-groups/gid! users&groups (make-hash)))

(define (groupinfo/name users&groups name)
  (hash-ref (ftp-users&groups-groups/name users&groups) name #f))

(define (groupinfo/uid users&groups gid)
  (hash-ref (ftp-users&groups-groups/gid users&groups) gid #f))

(define (group-name->gid users&groups name)
  (let ([group (hash-ref (ftp-users&groups-groups/name users&groups) name #f)])
    (and group (ftp-group-gid group))))

(define (gid->group-name users&groups gid)
  (let ([group (hash-ref (ftp-users&groups-groups/gid users&groups) gid #f)])
    (and group (ftp-group-name group))))

(define (make-ftp-permissions perm)
  (let-values ([(l? r? a? c? m? f? d?) (values #f #f #f #f #f #f #f)]
               [(correct?) #t])
    (and (symbol? perm)
         (for-each (λ (p)
                     (case p
                       [(#\l) (set! l? #t)]
                       [(#\r) (set! r? #t)]
                       [(#\a) (set! a? #t)]
                       [(#\c) (set! c? #t)]
                       [(#\m) (set! m? #t)]
                       [(#\f) (set! f? #t)]
                       [(#\d) (set! d? #t)]
                       [else (set! correct? #f)]))
                   (string->list (symbol->string perm)))
         correct?
         (ftp-permissions l? r? a? c? m? f? d?))))

(define (ftp-useradd users&groups login pass uid gid [ftp-perm #f] [root-dir "/"] [home-dirs '("/")] [info ""])
  (let ([root-dir (delete-lrws root-dir)])
    (let ([user (ftp-user login pass uid gid 
                          (make-ftp-permissions ftp-perm)
                          root-dir home-dirs 
                          info)])
      (hash-set! (ftp-users&groups-users/login users&groups) login user)
      (hash-set! (ftp-users&groups-users/uid users&groups) uid user))
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

(define (ftp-groupadd users&groups name gid users)
  (let ([users (ftp-group name gid (make-hash (map (λ(u) (cons u #t)) users)))])
    (hash-set! (ftp-users&groups-groups/name users&groups) name users)
    (hash-set! (ftp-users&groups-groups/gid users&groups) gid users)))

(define (real-path->ftp-path real-path root-dir [drop-tail-elem 0])
  (simplify-ftp-path (substring real-path (string-length root-dir)) drop-tail-elem))

(define (ftp-sysfile/full-info sys-file)
  (call-with-input-file sys-file
    (λ (in)
      (vector (integer-bytes->integer (read-bytes 2 in) #f)     ; sysbytes
              (integer-bytes->integer (read-bytes 4 in) #f)     ; uid
              (integer-bytes->integer (read-bytes 4 in) #f))))) ; gid

(define (ftp-sysfile/sysbytes sys-file)
  (call-with-input-file sys-file
    (λ (in)
      (integer-bytes->integer (read-bytes 2 in) #f))))

(define (ftp-sysfile/sysbytes+uid sys-file)
  (call-with-input-file sys-file
    (λ (in)
      (vector (integer-bytes->integer (read-bytes 2 in) #f)
              (integer-bytes->integer (read-bytes 4 in) #f)))))

(define (ftp-mkdir spath [uid 0][gid root-gid][permissions #b111101101])
  (unless (directory-exists? spath)
    (make-directory spath))
  (ftp-mksysfile (dir-path->sysfile-path spath)
                 uid gid permissions))

(define (ftp-mksysfile sysfile [uid 0][gid root-gid][permissions #b110100100])
  (call-with-output-file sysfile
    (λ (out)
      [write-bytes (integer->integer-bytes permissions 2 #f) out]
      [write-bytes (integer->integer-bytes uid 4 #f) out]
      [write-bytes (integer->integer-bytes gid 4 #f) out])
    #:exists 'truncate))

(define (get-sysfile-path/file spath)
  (file-path->sysfile-path spath))

(define (get-sysfile-path/dir spath)
  (dir-path->sysfile-path spath))

(define (ftp-dir-exists? spath)
  (and (directory-exists? spath)
       (file-exists? (dir-path->sysfile-path spath))))

(define (ftp-file-exists? spath)
  (and (file-exists? spath)
       (file-exists? (file-path->sysfile-path spath))))

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

(define (member-ftp-group/gid? clientStruct gid)
  (or (= gid (ftp-user-gid (ftp-client-userStruct clientStruct)))
      (and (hash-ref (ftp-groups/gid clientStruct) gid #f)
           (hash-ref (ftp-group-users (hash-ref (ftp-groups/gid clientStruct) gid))
                     (ftp-user-login (ftp-client-userStruct clientStruct)) #f))))

(define (member-ftp-group/name? clientStruct group-name)
  (member-ftp-group/gid? clientStruct 
                         (group-name->gid (ftp-client-users&groups clientStruct) group-name)))

(define (ftp-vfs-obj-allow-read? full-ftp-sysfile-spath clientStruct)
  (let ([info (ftp-sysfile/full-info full-ftp-sysfile-spath)])
    (cond
      ((= (vector-ref info 1) (ftp-user/uid clientStruct))
       (bitwise-bit-set? (vector-ref info 0) 8))
      ((member-ftp-group/gid? clientStruct (vector-ref info 2))
       (bitwise-bit-set? (vector-ref info 0) 5))
      (else
       (bitwise-bit-set? (vector-ref info 0) 2)))))

(define (ftp-vfs-obj-allow-write? full-ftp-sysfile-spath clientStruct)
  (let ([info (ftp-sysfile/full-info full-ftp-sysfile-spath)])
    (cond
      ((= (vector-ref info 1) (ftp-user/uid clientStruct))
       (bitwise-bit-set? (vector-ref info 0) 7))
      ((member-ftp-group/gid? clientStruct (vector-ref info 2))
       (bitwise-bit-set? (vector-ref info 0) 4))
      (else
       (bitwise-bit-set? (vector-ref info 0) 1)))))

(define (ftp-vfs-obj-allow-execute? full-ftp-sysfile-spath clientStruct)
  (let ([info (ftp-sysfile/full-info full-ftp-sysfile-spath)])
    (cond
      ((= (vector-ref info 1) (ftp-user/uid clientStruct))
       (bitwise-bit-set? (vector-ref info 0) 6))
      ((member-ftp-group/gid? clientStruct (vector-ref info 2))
       (bitwise-bit-set? (vector-ref info 0) 3))
      (else
       (bitwise-bit-set? (vector-ref info 0) 0)))))

(define (ftp-vfs-obj-access-allow? ftp-root-dir 
                                   ftp-full-spath 
                                   clientStruct
                                   [drop-tail-elem 0])
  (or (member-ftp-group/gid? clientStruct root-gid)
      (letrec ([test
                (λ(curr dirlist)
                  (if (null? dirlist)
                      #t
                      (let ([dir (string-append curr "/" (car dirlist))])
                        (and (ftp-vfs-obj-allow-execute? (dir-path->sysfile-path dir) clientStruct)
                             (test dir (cdr dirlist))))))])
        (let ([dirs (filter (λ (s) (not (string=? s "")))
                            (regexp-split #rx"[/\\\\]+" (simplify-path ftp-full-spath #f)))])
          (and (ftp-vfs-obj-allow-execute? (dir-path->sysfile-path ftp-root-dir) clientStruct)
               (or (null? dirs)
                   (test ftp-root-dir (drop-right dirs drop-tail-elem))))))))

(define (ftp-dir-allow-list? spath clientStruct)
  (or (member-ftp-group/gid? clientStruct root-gid)
      (and (if (ftp-user/ftp-perm clientStruct)
               (ftp-permissions-l? (ftp-user/ftp-perm clientStruct))
               #t)
           (ftp-vfs-obj-allow-read? (dir-path->sysfile-path spath) clientStruct))))

(define (ftp-dir-allow-store&append? spath clientStruct)
  (or (member-ftp-group/gid? clientStruct root-gid)
      (and (if (ftp-user/ftp-perm clientStruct)
               (ftp-permissions-c? (ftp-user/ftp-perm clientStruct))
               #t)
           (ftp-vfs-obj-allow-write? (dir-path->sysfile-path spath) clientStruct))))

(define (ftp-dir-allow-mkdir? spath clientStruct)
  (or (member-ftp-group/gid? clientStruct root-gid)
      (and (if (ftp-user/ftp-perm clientStruct)
               (ftp-permissions-m? (ftp-user/ftp-perm clientStruct))
               #t)
           (ftp-vfs-obj-allow-write? (dir-path->sysfile-path spath) clientStruct))))

(define (ftp-dir-allow-rename? spath clientStruct)
  (or (member-ftp-group/gid? clientStruct root-gid)
      (and (if (ftp-user/ftp-perm clientStruct)
               (ftp-permissions-f? (ftp-user/ftp-perm clientStruct))
               #t)
           (ftp-vfs-obj-allow-write? (dir-path->sysfile-path spath) clientStruct))))

(define (ftp-dir-allow-delete? spath clientStruct)
  (or (member-ftp-group/gid? clientStruct root-gid)
      (and (if (ftp-user/ftp-perm clientStruct)
               (ftp-permissions-d? (ftp-user/ftp-perm clientStruct))
               #t)
           (ftp-vfs-obj-allow-write? (dir-path->sysfile-path spath) clientStruct))))

(define (ftp-dir-allow-execute? spath clientStruct)
  (or (member-ftp-group/gid? clientStruct root-gid)
      (ftp-vfs-obj-allow-execute? (dir-path->sysfile-path spath) clientStruct)))

(define (ftp-file-allow-read? spath clientStruct)
  (or (member-ftp-group/gid? clientStruct root-gid)
      (ftp-vfs-obj-allow-read? (file-path->sysfile-path spath) clientStruct)))

(define (ftp-file-allow-append? spath clientStruct)
  (or (member-ftp-group/gid? clientStruct root-gid)
      (and (if (ftp-user/ftp-perm clientStruct)
               (ftp-permissions-a? (ftp-user/ftp-perm clientStruct))
               #t)
           (ftp-vfs-obj-allow-write? (file-path->sysfile-path spath) clientStruct))))