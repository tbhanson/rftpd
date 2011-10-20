#|

ProRFTPd Posix Library v1.4
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

(require ffi/unsafe)

(provide (except-out (all-defined-out)
		     libcrypt
		     libc))

(define-syntax (define-lib stx)
  (syntax-case stx ()
    [(_ lib path)
     (with-syntax ([deflib (datum->syntax
                            stx
                            (string->symbol
                             (string-append "def-"
                                            (symbol->string
                                             (syntax->datum #'lib)))))])
       #'(begin
           (define lib (ffi-lib path))
           (define-syntax-rule (deflib fun type)
             (define-c fun lib (_fun #:save-errno 'posix . type)))))]))

(define-syntax-rule (defconst name value)
  (define-syntax (name so) #'value))

(define-syntax-rule (typedef newtype type)
  (defconst newtype type))

(define-lib libcrypt "libcrypt.so.1")
(define-lib libc "libc.so.6")

(typedef pid_t      _int)
(typedef mode_t     _uint32)
(typedef nlink_t    _uint32)
(typedef ino_t      _ulong)
(typedef time_t     _long)
(typedef off_t      _long)
(typedef blksize_t  _long)
(typedef blkcnt_t   _long)
(typedef dev_t      _uint64)
(typedef uid_t      _uint32)
(typedef gid_t      _uint32)

(define-cstruct _Stat ([dev dev_t]
                       [__pad1 _ushort]
                       [ino ino_t]
                       [mode mode_t]
                       [nlink nlink_t]
                       [uid uid_t]
                       [gid gid_t]
                       [rdev dev_t]
                       [__pad2 _ushort]
                       [size off_t]
                       [blksize blksize_t]
                       [blocks blkcnt_t]
                       [atime time_t]
                       [atimensec _ulong]
                       [mtime time_t]
                       [mtimensec _ulong]
                       [ctime time_t]
                       [ctimensec _ulong]
                       [__unused _uint64]))

(define-cstruct _Passwd ([name _string]
                         [passwd _string]
                         [uid uid_t]
                         [gid gid_t]
                         [gecos _string]
                         [home _string]
                         [shell _string]))

(define-cstruct _Group ([name _string]
                        [passwd _string]
                        [gid gid_t]
                        [members _pointer]))

(define-cstruct _Spwd ([name _string]
                       [passwd _string]
                       [lstchg _long]
                       [min _long]
                       [max _long]
                       [warn _long]
                       [inact _long]
                       [expire _long]
                       [flag _ulong]))

(define-cstruct _Utimbuf ([actime time_t]
                          [modtime time_t]))

(def-libcrypt crypt (_string _string -> _string))

(def-libc daemon (_int _int -> _int))

(def-libc getuid (-> _int))
(def-libc getgid (-> _int))
(def-libc geteuid (-> _int))
(def-libc getegid (-> _int))

(def-libc setuid (uid_t -> _int))
(def-libc setgid (gid_t -> _int))
(def-libc seteuid (uid_t -> _int))
(def-libc setegid (gid_t -> _int))

(def-libc access (_string _int -> _int))
(def-libc eaccess (_string _int -> _int))
; int euidaccess(const char *pathname, int mode);

(defconst STAT-VER-LINUX 3)
(def-libc __xstat (_int _string _Stat-pointer -> _int))
(def-libc __lxstat (_int _string _Stat-pointer -> _int))

(def-libc utime (_string _Utimbuf-pointer -> _int))

(def-libc mkdir (_string mode_t -> _int))
(def-libc rmdir (_string -> _int))

(def-libc unlink (_string -> _int))

(def-libc getgrgid (gid_t -> (_or-null _Group-pointer)))
(def-libc getgrnam (_string -> (_or-null _Group-pointer)))

(def-libc getpwuid (uid_t -> (_or-null _Passwd-pointer)))
(def-libc getpwnam (_string -> (_or-null _Passwd-pointer)))
(def-libc getspnam (_string -> (_or-null _Spwd-pointer)))

(def-libc chown (_string uid_t gid_t -> _int))
(def-libc lchown (_string uid_t gid_t -> _int))
(def-libc chmod (_string mode_t -> _int))
