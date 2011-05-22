#|

ProRFTPd System Library v1.5
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

(require (prefix-in ffi: ffi/unsafe)
         (file "lib-string.rkt")
         (file "platform.rkt"))

(provide (except-out (all-defined-out)
                     (struct-out ftp-users)
                     crypt-string
                     change-egid&euid
                     change*-egid&euid
                     restore-euid&egid))

(provide/contract
 [crypt-string (string? string? . -> . string?)])

(struct ftp-user (login real-user anonymous? uid gid ftp-perm root-dir))
(struct ftp-users (logins uids) #:mutable)
(struct ftp-permissions (l? r? a? c? m? f? d?))

(struct exn:posix exn (id errno) #:transparent)

(defconst root-uid 0)
(defconst root-gid 0)

(define chids/sem (make-semaphore 1))

(define (make-users-table)
  (ftp-users (make-hash) (make-hash)))

(define-syntax-rule (throw-errno* fun errno)
  (raise (exn:posix (format "~a: errno=~a" 'fun errno) 
                    (current-continuation-marks) 
                    'fun errno)))

(define-syntax-rule (throw-errno fun) (throw-errno* fun (ffi:saved-errno)))

(define (clear-users-info users)
  (set-ftp-users-logins! users (make-hash))
  (set-ftp-users-uids! users (make-hash)))

(define (userinfo/login users login)
  (hash-ref (ftp-users-logins users) login #f))

(define (userinfo/uid users uid)
  (hash-ref (ftp-users-uids users) uid #f))

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

(define (ftp-useradd users login real-user anonymous? [ftp-perm #f] [root-dir "/"])
  (let ([root-dir (and root-dir (delete-lrws root-dir))]
        [passwdStruct (getpwnam real-user)])
    (if passwdStruct
        (let ([user (ftp-user login
                              real-user
                              anonymous?
                              (Passwd-uid passwdStruct) 
                              (Passwd-gid passwdStruct) 
                              (make-ftp-permissions ftp-perm)
                              (or root-dir (Passwd-home passwdStruct)))])
          (hash-set! (ftp-users-logins users) login user)
          (hash-set! (ftp-users-uids users) (Passwd-uid passwdStruct) user)
          (and root-dir
               (unless (directory-exists? root-dir)
                 (ftp-mkdir root-dir 
                            (Passwd-uid passwdStruct) 
                            (Passwd-gid passwdStruct))))
          (void))
        (error 'ftp-useradd "user ~a not found." login))))

(define (real-path->ftp-path real-path root-dir [drop-tail-elem 0])
  (simplify-ftp-path (substring real-path (string-length root-dir)) drop-tail-elem))

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

(define (activate-daemon-mode nochdir noclose)
  (unless (zero? (daemon (if nochdir -1 0) (if noclose -1 0)))
    (throw-errno daemon)))

(define (set!-euid&egid euid egid)
  (unless (zero? (seteuid euid))
    (throw-errno seteuid))
  (unless (zero? (setegid egid))
    (throw-errno setegid)))

(define (set!-egid&euid egid euid)
  (unless (zero? (setegid egid))
    (throw-errno setegid))
  (unless (zero? (seteuid euid))
    (throw-errno seteuid)))

;(define (ftp-chmod spath mode)
;  (unless (zero? (chmod spath mode))
;    (throw-errno chmod)))

(define (ftp-chown spath uid gid)
  (unless (zero? (chown spath uid gid))
    (throw-errno chown)))

(define-syntax-rule (change-egid&euid egid euid)
  (with-handlers ([any/c (λ(e) (exit 1))])
    (semaphore-wait chids/sem)
    (set!-egid&euid egid euid)))

(define-syntax-rule (change*-egid&euid userstruct)
  (with-handlers ([any/c (λ(e) (exit 1))])
    (semaphore-wait chids/sem)
    (set!-egid&euid (ftp-user-gid userstruct) (ftp-user-uid userstruct))))

(define-syntax-rule (restore-euid&egid)
  (with-handlers ([any/c (λ(e) (exit 1))])
    (set!-euid&egid (getuid) (getgid))
    (semaphore-post chids/sem)))

(define (ftp-mkdir spath [uid root-uid][gid root-gid] #:mode [mode #o755])
  (let ([errno #f])
    (change-egid&euid gid uid)
    (unless (zero? (mkdir spath mode))
      (set! errno (ffi:saved-errno)))
    (restore-euid&egid)
    (when errno (throw-errno* mkdir errno))))

(define (ftp-utime* userstruct spath actime modtime)
  (let ([utimbuf (make-Utimbuf actime modtime)]
        [errno #f])
    (change*-egid&euid userstruct)
    (unless (zero? (utime spath utimbuf))
      (set! errno (ffi:saved-errno)))
    (restore-euid&egid)
    (when errno (throw-errno* utime errno))))

(define (ftp-rmdir* userstruct spath)
  (let ([errno #f])
    (change*-egid&euid userstruct)
    (unless (zero? (rmdir spath))
      (set! errno (ffi:saved-errno)))
    (restore-euid&egid)
    (when errno (throw-errno* rmdir errno))))

(define (ftp-unlink* userstruct spath)
  (let ([errno #f])
    (change*-egid&euid userstruct)
    (unless (zero? (unlink spath))
      (set! errno (ffi:saved-errno)))
    (restore-euid&egid)
    (when errno (throw-errno* unlink errno))))

(define (ftp-chmod* userstruct spath mode)
  (let ([errno #f])
    (change*-egid&euid userstruct)
    (unless (zero? (chmod spath mode))
      (set! errno (ffi:saved-errno)))
    (restore-euid&egid)
    (when errno (throw-errno* chmod errno))))

(define (ftp-chown* userstruct spath uid gid)
  (let ([errno #f])
    (change*-egid&euid userstruct)
    (unless (zero? (chown spath uid gid))
      (set! errno (ffi:saved-errno)))
    (restore-euid&egid)
    (when errno (throw-errno* chmod errno))))

(define (ftp-access* userstruct spath mode)
  (let ([result #f])
    (change*-egid&euid userstruct)
    (set! result (zero? (eaccess spath mode)))
    (restore-euid&egid)
    result))

(define (ftp-stat* userstruct spath)
  (let ([st (make-Stat 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0)]
        [errno #f])
    (change*-egid&euid userstruct)
    (unless (zero? (__xstat STAT-VER-LINUX spath st))
      (set! errno (ffi:saved-errno)))
    (restore-euid&egid)
    (when errno (throw-errno* stat errno))
    st))

(define (get-shadow-passwd login)
  (let ([spwd (getspnam login)])
    (and spwd (Spwd-passwd spwd))))

(define (crypt-string str salt)
  (crypt str salt))

(define (check-user-pass login pass)
  (let ([spwd (getspnam login)])
    (and spwd
         (let* ([passwd (Spwd-passwd spwd)]
                [salt (let ([l (regexp-split #rx"\\$" passwd)])
                        (format "$~a$~a" (second l) (third l)))]
                [pass (crypt pass salt)])
           (and pass (string=? passwd pass))))))

(define (ftp-stat spath)
  (let ([st (make-Stat 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0)])
    (and (zero? (__xstat STAT-VER-LINUX spath st))
         st)))

(define (ftp-lstat spath)
  (let ([st (make-Stat 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0)])
    (and (zero? (__lxstat STAT-VER-LINUX spath st))
         st)))

(define (uid->uname uid)
  (let ([pwd (getpwuid uid)])
    (and pwd
         (Passwd-name pwd))))

(define (login->uid login)
  (let ([pwd (getpwnam login)])
    (and pwd
         (Passwd-uid pwd))))

(define (gid->gname gid)
  (let ([grp (getgrgid gid)])
    (and grp
         (Group-name grp))))

(define (gname->gid gname)
  (let ([grp (getgrnam gname)])
    (and grp
         (Group-gid grp))))

(define (grpmember? gid-or-grpname uid-or-usrname)
  (let ([grp (if (number? gid-or-grpname)
                 (getgrgid gid-or-grpname)
                 (getgrnam gid-or-grpname))]
        [uname (if (number? uid-or-usrname)
                   (uid->uname uid-or-usrname)
                   uid-or-usrname)])
    (and grp uname
         (or (string=? (Group-name grp) uname)
             (let ([*mem (Group-members grp)])
               (let loop ([offs 0])
                 (and (ffi:ptr-ref *mem ffi:_string offs)
                      (or (string=? (ffi:ptr-ref *mem ffi:_string offs) uname)
                          (loop (add1 offs))))))))))

(define (stat->user-mode* userstruct stat)
  (cond
    [(= (ftp-user-uid userstruct) (Stat-uid stat))
     (arithmetic-shift (bitwise-and (Stat-mode stat) #o700) -6)]
    [(grpmember? (Stat-gid stat) (ftp-user-uid userstruct))
     (arithmetic-shift (bitwise-and (Stat-mode stat) #o70) -3)]
    [else (bitwise-and (Stat-mode stat) 7)]))

(define (stat->user-mode** userstruct stat)
  (cond
    [(= (ftp-user-uid userstruct) (Stat-uid stat))
     (arithmetic-shift (bitwise-and (Stat-mode stat) #o700) -6)]
    [(grpmember? (Stat-gid stat) (ftp-user-uid userstruct))
     (arithmetic-shift (bitwise-and (Stat-mode stat) #o70) -3)]
    [(grpmember? root-gid (ftp-user-uid userstruct))
     (bitwise-ior 6
                  (arithmetic-shift (bitwise-and (Stat-mode stat) #o700) -6)
                  (arithmetic-shift (bitwise-and (Stat-mode stat) #o70) -3)
                  (bitwise-and (Stat-mode stat) 7))]
    [else (bitwise-and (Stat-mode stat) 7)]))