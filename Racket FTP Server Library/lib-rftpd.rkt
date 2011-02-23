#|

Racket FTP Server Library v1.2.8
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

(require racket/date
         (prefix-in srfi/19: srfi/19)
         (file "lib-ssl.rkt")
         srfi/48)

(provide ftp-server%
         and/exc
         IPv4?
         IPv6?
         host-string?
         port-number?
         ssl-protocol?
         make-passive-ports)

(struct ftp-user (full-name login pass group home-dirs root-dir))
(struct ftp-host&port (host port))
(struct passive-ports (from to))
(struct ftp-mlst-features (size? modify? perm?) #:mutable)

(struct ftp-server-params
  (passive-1-ports
   passive-2-ports
   
   server-1-host
   server-2-host
   
   ;server-1-encryption
   ;server-2-encryption
   
   ;server-1-certificate
   ;server-2-certificate
   
   ;server-1-ssl-context
   ;server-2-ssl-context
   
   server-responses
   
   default-root-dir
   
   default-locale-encoding
   
   log-output-port
   
   ftp-users
   ftp-groups
   
   bad-auth
   
   bad-auth-sleep
   max-auth-attempts
   
   pass-sleep))

(date-display-format 'iso-8601)

;;
;; ---------- Global Definitions ----------
;;
(define ftp-run-date (srfi/19:current-date))
(define ftp-date-zone-offset (srfi/19:date-zone-offset ftp-run-date))
(define ftp-vfs-file-extension #"ftp-racket-file")
(define ftp-vfs-dir-extension #"ftp-racket-directory")
(define ftp-vfs-file-spath ".ftp-racket-file")
(define ftp-vfs-dir-spath "/.ftp-racket-directory")
(define random-gen (make-pseudo-random-generator))

(define default-server-responses
  (make-hash
   '((SYNTAX-ERROR (EN . "501 ~a Syntax error in parameters or arguments.")
                   (RU . "501 ~a Синтаксическая ошибка (неверный параметр или аргумент)."))
     (WELCOME (EN . "220 ~a")
              (RU . "220 ~a"))
     (CMD-NOT-IMPLEMENTED (EN . "502 ~a not implemented.")
                          (RU . "502 Команда ~a не реализована."))
     (PLEASE-LOGIN (EN . "530 Please login with USER and PASS.")
                   (RU . "530 Пожалуйста авторизируйтесь используя USER и PASS."))
     (ANONYMOUS-LOGIN (EN . "331 Anonymous login ok, send your complete email address as your password.")
                      (RU . "331 Анонимный логин корректен, в качестве пароля используйте Ваш email."))
     (PASSW-REQUIRED (EN . "331 Password required for ~a")
                     (RU . "331 Введите пароль для пользователя ~a"))
     (LOGIN-INCORRECT (EN . "530 Login incorrect.")
                      (RU . "530 Вход не выполнен (введены не верные данные)."))
     (ANONYMOUS-LOGGED (EN . "230 Anonymous access granted.")
                       (RU . "230 Доступ анонимному пользователю предоставлен."))
     (USER-LOGGED (EN . "230 User ~a logged in.")
                  (RU . "230 Пользователь ~a успешно прошел идентификацию."))
     (SERVICE-READY (EN . "220 Service ready for new user.")
                    (RU . "220 Служба подготовлена для следующей авторизации."))
     (QUIT (EN . "221 Goodbye.")
           (RU . "221 До свидания."))
     (ABORT (EN . "226 Abort successful.")
            (RU . "226 Текущая операция прервана."))
     (SYSTEM (EN . "215 UNIX (Unix-like)")
             (RU. "215 UNIX (Unix-подобная)"))
     (CURRENT-DIR (EN . "257 ~s is current directory.")
                  (RU . "257 ~s - текущий каталог."))
     (CMD-SUCCESSFUL (EN . "~a ~a command successful.")
                     (RU . "~a Команда ~a выполнена."))
     (END (EN . "~a End")
          (RU . "~a Конец"))
     (FEAT-LIST (EN . "211-Extensions supported:")
                (RU . "211-Поддерживаемые расширения:"))
     (RESTART (EN . "350 Restart marker accepted.")
              (RU . "350 Рестарт-маркер установлен."))
     (STATUS-LIST (EN . "213-Status of ~s:")
                  (RU . "213-Статус ~s:"))
     (STATUS-INFO-1 (EN . "211-FTP Server status:")
                    (RU . "211-Статус FTP Сервера:"))
     (STATUS-INFO-2 (EN . " Connected to ~a")
                    (RU . " Подключен к ~a"))
     (STATUS-INFO-3 (EN . " Logged in as ~a")
                    (RU . " Вы вошли как ~a"))
     (STATUS-INFO-4 (EN . " TYPE: ~a; STRU: ~a; MODE: ~a")
                    (RU . " TYPE: ~a; STRU: ~a; MODE: ~a"))
     (SET-CMD (EN . "~a ~a set to ~a.")
              (RU . "~a ~a установлен в ~a."))
     (MISSING-PARAMS (EN . "504 Command not implemented for that parameter.")
                     (RU . "504 Команда не применима для такого параметра."))
     (DIR-NOT-FOUND (EN . "550 Directory not found.")
                    (RU . "550 Каталог отсутствует."))
     (PERM-DENIED (EN . "550 Permission denied.")
                  (RU . "Доступ запрещен."))
     (UNSUPTYPE (EN . "501 Unsupported type. Supported types are I and A.")
                (RU . "501 Неподдерживаемый тип. Поддерживаемые типы I и А."))
     (UNKNOWN-TYPE (EN . "501 Unknown ~a type.")
                   (RU . "501 Неизвестный ~a тип."))
     (DIR-EXIST (EN . "550 Can't create directory. Directory exist!")
                (RU . "550 Невозможно создать каталог. Каталог существует!"))
     (DIR-CREATED (EN . "257 ~s - directory successfully created.")
                  (RU . "257 ~s - создан каталог."))
     (CREATE-DIR-PERM-DENIED (EN . "550 Can't create directory. Permission denied!")
                             (RU . "550 Невозможно создать каталог. Доступ запрещен!"))
     (CANT-CREATE-DIR (EN . "550 Can't create directory.")
                      (RU . "550 Невозможно создать каталог."))
     (DELDIR-NOT-EMPTY (EN . "550 Can't delete directory. Directory not empty!")
                       (RU . "550 Не удается удалить каталог. Каталог не пуст!"))
     (DELDIR-PERM-DENIED (EN . "550 Can't delete directory. Permission denied!")
                         (RU ."550 Не удается удалить каталог. Доступ запрещен!"))
     (STORE-FILE-PERM-DENIED (EN . "550 Can't store file. Permission denied!")
                             (RU . "550 Невозможно сохранить файл. Доступ запрещен!"))
     (CANT-STORE-FILE (EN . "550 Can't store file.")
                      (RU . "550 Невозможно сохранить файл."))
     (DELFILE-PERM-DENIED (EN . "550 Can't delete file. Permission denied!")
                          (RU . "550 Не удается удалить файл. Доступ запрещен!"))
     (FILE-NOT-FOUND (EN . "550 File not found.")
                     (RU . "550 Файл не найден."))
     (FILE-DIR-NOT-FOUND (EN . "550 File or directory not found.")
                         (RU . "550 Файл или каталог отсутствует."))
     (MLST-LISTING (EN . "250-Listing:")
                   (RU . "250-Листинг:"))
     (UTF8-ON (EN . "200 UTF8 mode enabled.")
              (RU . "200 Включен режим UTF8."))
     (UTF8-OFF (EN . "200 UTF8 mode disabled.")
               (RU . "200 Отключен режим UTF8."))
     (MLST-ON (EN . "200 MLST modes enabled.")
              (RU . "200 Включены все доступные MLST режимы."))
     (RENAME-OK (EN . "350 File or directory exists, ready for destination name.")
                (RU . "350 Файл или каталог существует, ожидается переименование."))
     (CANT-RENAME-EXIST (EN . "550 File or directory exist.")
                        (RU . "550 Обнаружен файл или каталог с подобным именем."))
     (CANT-RENAME (EN . "550 Can't rename file or directory.")
                  (RU . "550 Невозможно переименовать файл или каталог."))
     (RENAME-PERM-DENIED (EN . "550 Can't rename file or directory. Permission denied!")
                         (RU . "550 Невозможно переименовать файл или каталог. Доступ запрещен!"))
     (CMD-BAD-SEQ (EN . "503 Bad sequence of commands.")
                  (RU . "503 Соблюдайте последовательность команд!"))
     (PASV (EN . "227 Entering Passive Mode (~a,~a,~a,~a,~a,~a)")
           (RU . "227 Переход в Пассивный Режим (~a,~a,~a,~a,~a,~a)"))
     (UNKNOWN-CMD (EN . "501 Unknown command ~a.")
                  (RU . "501 Неизвестная команда ~a."))
     (HELP (EN . "214 Syntax: ~a")
           (RU . "214 Синтаксис: ~a"))
     (HELP-LISTING (EN . "214-The following commands are recognized:")
                   (RU . "214-Реализованы следующие команды:"))
     (TRANSFER-ABORTED (EN . "426 Connection closed; transfer aborted.")
                       (RU . "426 Соединение закрыто; передача прервана."))
     (OPEN-DATA-CONNECTION (EN . "150 Opening ~a mode data connection.")
                           (RU . "150 Открыт ~a режим передачи данных."))
     (TRANSFER-OK (EN . "226 Transfer complete.")
                  (RU . "226 Передача завершена."))
     (CLNT (EN . "200 Don't care.")
           (RU . "200 Не имеет значения."))
     (EPSV (EN . "229 Entering Extended Passive Mode (|||~a|)")
           (RU . "229 Переход в Расширенный Пассивный Режим (|||~a|)"))
     (BAD-PROTOCOL (EN . "522 Bad network protocol.")
                   (RU . "522 Неверный сетевой протокол.")))))

(define-syntax (and/exc stx)
  (syntax-case stx ()
    [(_ expr ...)
     #'(with-handlers ([any/c (λ (e) #f)])
         (and expr ...))]))

(define (IPv4? ip)
  (with-handlers ([any/c (λ (e) #f)])
    (let ([l (regexp-split #rx"\\." ip)])
      (and ((length l) . = . 4)
           (andmap (λ(s) (byte? (string->number s))) l)))))

(define (IPv6? ip)
  (and/exc (regexp-match #rx"^[0-9a-fA-F]+:|^::" ip)
           (regexp-match #rx":[0-9a-fA-F]+$|::$" ip)
           (not (regexp-match #rx":::" ip))
           (let ([l (regexp-split #rx":" ip)])
             (and (or (and (regexp-match #rx"::" ip)
                           ((length l) . <= . 8))
                      ((length l) . = . 8))
                  (andmap (λ(s)
                            (if (string=? s "")
                                #t
                                ((string->number s 16). <= . #xFFFF)))
                          l)))))

(define (host-string? host)
  (and (string? host)
       (or (IPv4? host) (IPv6? host) (string-ci=? host))))

(define (port-number? port)
  (and (exact-nonnegative-integer? port) (port . <= . #xffff)))

(define (ssl-protocol? prt)
  (memq prt '(sslv2-or-v3 sslv2 sslv3 tls)))

(define (make-passive-ports from to)
  (and (port-number? from) (port-number? to) (from . < . to)
       (passive-ports from to)))

(define (short-print-log-event log-out client-host user-id msg [user-name? #t])
  (if user-name?
      (fprintf log-out
               "~a [~a] ~a : ~a\n" (date->string (current-date) #t) client-host user-id msg)
      (fprintf log-out
               "~a [~a] ~a\n" (date->string (current-date) #t) client-host msg)))

(define (alarm-clock period count [event (λ() 1)])
  (let* ([tick count]
         [reset (λ () (set! tick count))]
         [thd (thread (λ ()
                        (do () [(<= tick 0) (event)]
                          (sleep period)
                          (set! tick (sub1 tick)))))]
         [kill (λ() (kill-thread thd))])
    (values reset kill)))

(define ftp-vfs%
  (mixin () ()
    (super-new)
    
    (define/public (real-path->ftp-path real-path root-dir [drop-tail-elem 0])
      (simplify-ftp-path (substring real-path (string-length root-dir)) drop-tail-elem))
    
    (define/public (ftp-file-or-dir-full-info sys-file)
      (call-with-input-file sys-file
        (λ (in)
          (vector (integer-bytes->integer (read-bytes 2 in) #f) ; sysbytes
                  (read-line in)                                ; owner
                  (read-line in)))))                            ; group
    
    (define/public (ftp-file-or-dir-sysbytes sys-file)
      (call-with-input-file sys-file
        (λ (in)
          (integer-bytes->integer (read-bytes 2 in) #f))))
    
    (define/public (ftp-file-or-dir-sysbytes/owner sys-file)
      (call-with-input-file sys-file
        (λ (in)
          (vector (integer-bytes->integer (read-bytes 2 in) #f)
                  (read-line in)))))
    
    (define/public (ftp-mkdir* spath [owner "racket"][group "racket"][permissions #b111101101])
      (make-directory spath)
      (ftp-mksys-file (string-append spath ftp-vfs-dir-spath)
                      owner group permissions))
    
    (define/public (ftp-mksys-file sys-file [owner "racket"][group "racket"][permissions #b111101101])
      (call-with-output-file sys-file
        (λ (out)
          [write-bytes (integer->integer-bytes permissions 2 #f) out]
          [display owner out][newline out]
          [display group out][newline out])
        #:exists 'truncate))
    
    (define/public (ftp-dir-exists? spath)
      (and (directory-exists? spath)
           (file-exists? (string-append spath ftp-vfs-dir-spath))))
    
    (define/public (ftp-file-exists? spath)
      (and (file-exists? spath)
           (file-exists? (string-append spath ftp-vfs-file-spath))))
    
    (define/public (ftp-file-name-safe? spath)
      (not (and (filename-extension spath)
                (or (bytes=? (filename-extension spath) ftp-vfs-file-extension)
                    (bytes=? (filename-extension spath) ftp-vfs-dir-extension)))))
    
    (define/public (simplify-ftp-path ftp-path [drop-tail-elem 0])
      (with-handlers ([any/c (λ (e) "/")])
        (let ([path-lst (drop-right (filter (λ (s) (not (string=? s "")))
                                            (regexp-split #rx"[/\\\\]+" (simplify-path ftp-path #f)))
                                    drop-tail-elem)])
          (if (null? path-lst)
              "/"
              (foldr (λ (a b) (string-append "/" a b)) "" path-lst)))))
    
    (define/public (member-ftp-group? groups user group)
      (or (string=? group (ftp-user-group user))
          (and (hash-ref groups group #f)
               (hash-ref (hash-ref groups group) (ftp-user-login user) #f))))
    
    (define/public (ftp-allow-read? full-ftp-sys-file-spath user groups)
      (let ([info (ftp-file-or-dir-full-info full-ftp-sys-file-spath)])
        (cond
          ((string=? (vector-ref info 1) (ftp-user-login user))
           (bitwise-bit-set? (vector-ref info 0) 8))
          ((member-ftp-group? groups user (vector-ref info 2))
           (bitwise-bit-set? (vector-ref info 0) 5))
          (else
           (bitwise-bit-set? (vector-ref info 0) 2)))))
    
    (define/public (ftp-allow-write? full-ftp-sys-file-spath user groups)
      (let ([info (ftp-file-or-dir-full-info full-ftp-sys-file-spath)])
        (cond
          ((string=? (vector-ref info 1) (ftp-user-login user))
           (bitwise-bit-set? (vector-ref info 0) 7))
          ((member-ftp-group? groups user (vector-ref info 2))
           (bitwise-bit-set? (vector-ref info 0) 4))
          (else
           (bitwise-bit-set? (vector-ref info 0) 1)))))
    
    (define/public (ftp-allow-execute? full-ftp-sys-file-spath user groups)
      (let ([info (ftp-file-or-dir-full-info full-ftp-sys-file-spath)])
        (cond
          ((string=? (vector-ref info 1) (ftp-user-login user))
           (bitwise-bit-set? (vector-ref info 0) 6))
          ((member-ftp-group? groups user (vector-ref info 2))
           (bitwise-bit-set? (vector-ref info 0) 3))
          (else
           (bitwise-bit-set? (vector-ref info 0) 0)))))
    
    (define/public (ftp-allow*? oper full-ftp-sys-file-spath user groups)
      (let ([info (ftp-file-or-dir-full-info full-ftp-sys-file-spath)])
        (case oper
          [(read)
           (cond
             ((string=? (vector-ref info 1) (ftp-user-login user))
              (bitwise-bit-set? (vector-ref info 0) 8))
             ((member-ftp-group? groups user (vector-ref info 2))
              (bitwise-bit-set? (vector-ref info 0) 5))
             (else
              (bitwise-bit-set? (vector-ref info 0) 2)))]
          [(write)
           (cond
             ((string=? (vector-ref info 1) (ftp-user-login user))
              (bitwise-bit-set? (vector-ref info 0) 7))
             ((member-ftp-group? groups user (vector-ref info 2))
              (bitwise-bit-set? (vector-ref info 0) 4))
             (else
              (bitwise-bit-set? (vector-ref info 0) 1)))]
          [(execute)
           (cond
             ((string=? (vector-ref info 1) (ftp-user-login user))
              (bitwise-bit-set? (vector-ref info 0) 6))
             ((member-ftp-group? groups user (vector-ref info 2))
              (bitwise-bit-set? (vector-ref info 0) 3))
             (else
              (bitwise-bit-set? (vector-ref info 0) 0)))])))
    
    (define/public (ftp-access-allow? ftp-root-dir 
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
    
    (define/public (ftp-dir-allow-read? spath user groups)
      (ftp-allow-read? (string-append spath ftp-vfs-dir-spath) user groups))
    
    (define/public (ftp-dir-allow-write? spath user groups)
      (ftp-allow-write? (string-append spath ftp-vfs-dir-spath) user groups))
    
    (define/public (ftp-dir-allow-execute? spath user groups)
      (ftp-allow-execute? (string-append spath ftp-vfs-dir-spath) user groups))
    
    (define/public (ftp-file-allow-read? spath user groups)
      (ftp-allow-read? (string-append spath ftp-vfs-file-spath) user groups))
    
    (define/public (ftp-file-allow-write? spath user groups)
      (ftp-allow-write? (string-append spath ftp-vfs-file-spath) user groups))))

(define ftp-utils%
  (mixin () ()
    (super-new)
    
    (define/public (get-params req)
      (get-params* #rx"[^A-z]+.*" req))
    
    (define/public (get-params* delseq req)
      (let ([p (regexp-match delseq req)])
        (and p
             (let ([p (regexp-match #rx"[^ \t]+.*" (car p))])
               (and p
                    (let ([p (regexp-match #rx".*[^ \t]+" (car p))])
                      (and p (car p))))))))))

(define ftp-encoding%
  (mixin () ()
    (super-new)
    
    (init-field [default-encoding "UTF-8"])
    
    (define/public (bytes->bytes/encoding encoding bstr)
      (let*-values ([(conv) (bytes-open-converter encoding "UTF-8")]
                    [(result len status) (bytes-convert conv bstr)])
        (bytes-close-converter conv)
        (unless (eq? status 'complete)
          (set! conv (bytes-open-converter default-encoding "UTF-8"))
          (set!-values (result len status) (bytes-convert conv bstr))
          (bytes-close-converter conv))
        result))
    
    (define/public (bytes->string/encoding encoding bstr)
      (bytes->string/utf-8 (bytes->bytes/encoding encoding bstr)))
    
    (define/public (string->bytes/encoding encoding str)
      (bytes->bytes/encoding encoding (string->bytes/utf-8 str)))
    
    (define/public (print/encoding encoding str out)
      (write-bytes (string->bytes/encoding encoding str) out))
    
    (define/public (read-request encoding input-port)
      (and (byte-ready? input-port)
           (let ([line (read-bytes-line input-port)])
             (if (eof-object? line)
                 line
                 (let ([s (bytes->string/encoding encoding line)])
                   (substring s 0 (sub1 (string-length s))))))))
    
    (define/public (print-crlf/encoding encoding str out)
      (print/encoding encoding str out)
      (write-bytes #"\r\n" out)
      (flush-output out))
    
    (define/public (print*-crlf/encoding encoding out current-lang server-responses response-tag . args)
      (let ([response (cdr (assq current-lang (hash-ref server-responses response-tag)))])
        (print-crlf/encoding encoding (if (null? args) response (apply format response args)) out)))))

(define ftp-DTP%
  (class (ftp-encoding% (ftp-utils% (ftp-vfs% object%)))
    (inherit ftp-mksys-file)
    
    (init-field server-params
                current-server
                [pasv-listener #f]
                [ssl-server-context #f]
                [ssl-client-context #f]
                [current-process (make-custodian)]
                [active-host&port (ftp-host&port "127.0.0.1" 20)]
                [passive-host&port (ftp-host&port "127.0.0.1" 20)]
                [restart-marker #f]
                [DTP 'active]
                [representation-type 'ASCII]
                [transfer-mode 'Stream]
                [file-structure 'File]
                [print-abort (λ() #f)]
                [print-connect (λ() #f)]
                [print-close (λ() #f)]
                [print-ascii (λ(data out) #f)]
                [log-file-event (λ(new-file-full-path exists-mode) #f)])
    
    (super-new [default-encoding default-locale-encoding])
    
    (define net-connect (λ(host port)
                          (if ssl-client-context
                              (ssl-connect host port ssl-client-context)
                              (tcp-connect host port))))
    
    (define net-accept (λ(listener)
                         (let-values ([(in out) (tcp-accept listener)])
                           (if ssl-server-context 
                               (ports->ssl-ports in out 
                                                 #:mode 'accept
                                                 #:context ssl-server-context
                                                 #:close-original? #t
                                                 #:shutdown-on-close? #t) 
                               (values in out)))))
    
    (define/public (ftp-data-transfer data [file? #f])
      (case DTP
        ((passive)
         (passive-data-transfer data file?))
        ((active)
         (active-data-transfer data file?))))
    
    (define (active-data-transfer data file?)
      (let ([host (ftp-host&port-host active-host&port)]
            [port (ftp-host&port-port active-host&port)])
        (parameterize ([current-custodian current-process])
          (thread (λ ()
                    (with-handlers ([any/c (λ (e) (print-abort))])
                      (let-values ([(in out) (net-connect host port)])
                        (print-connect)
                        (let-values ([(reset-alarm kill-alarm)
                                      (alarm-clock 1 15
                                                   (λ()
                                                     ;(displayln "Auto kill working process!" log-output-port)
                                                     (custodian-shutdown-all current-process)))])
                          (if file?
                              (call-with-input-file data
                                (λ (in)
                                  (let loop ([dat (read-bytes 1048576 in)])
                                    (reset-alarm)
                                    (unless (eof-object? dat)
                                      (write-bytes dat out)
                                      (loop (read-bytes 1048576 in))))))
                              (case representation-type
                                ((ASCII)
                                 (print-ascii data out))
                                ((Image)
                                 (write-bytes data out))))
                          (kill-alarm))
                        (flush-output out)
                        ;(close-input-port in);ssl required
                        ;(close-output-port out);ssl required
                        (print-close)))
                    ;(displayln  "Process complete!" log-output-port)
                    (custodian-shutdown-all current-process))))))
    
    (define (passive-data-transfer data file?)
      (parameterize ([current-custodian current-process])
        (thread (λ ()
                  (with-handlers ([any/c (λ (e) (print-abort))])
                    (let-values ([(in out) (net-accept pasv-listener)])
                      (print-connect)
                      (let-values ([(reset-alarm kill-alarm)
                                    (alarm-clock 1 15
                                                 (λ()
                                                   ;(displayln "Auto kill working process!" log-output-port)
                                                   (custodian-shutdown-all current-process)))])
                        (if file?
                            (call-with-input-file data
                              (λ (in)
                                (let loop ([dat (read-bytes 1048576 in)])
                                  (reset-alarm)
                                  (unless (eof-object? dat)
                                    (write-bytes dat out)
                                    (loop (read-bytes 1048576 in))))))
                            (case representation-type
                              ((ASCII)
                               (print-ascii data out))
                              ((Image)
                               (write-bytes data out))))
                        (kill-alarm))
                      ;(flush-output out)
                      (close-output-port out);ssl required
                      (print-close)))
                  ;(displayln "Process complete!" log-output-port)
                  (custodian-shutdown-all current-process)))))
    
    (define/public (ftp-store-file current-ftp-user new-file-full-path exists-mode)
      (case DTP
        ((passive)
         (passive-store-file current-ftp-user new-file-full-path exists-mode))
        ((active)
         (active-store-file current-ftp-user new-file-full-path exists-mode))))
    
    (define (active-store-file current-ftp-user new-file-full-path exists-mode)
      (let ([host (ftp-host&port-host active-host&port)]
            [port (ftp-host&port-port active-host&port)])
        (parameterize ([current-custodian current-process])
          (thread (λ ()
                    (with-handlers ([any/c (λ (e) (print-abort))])
                      (call-with-output-file new-file-full-path
                        (λ (fout)
                          (unless (file-exists? (string-append new-file-full-path ftp-vfs-file-spath))
                            (ftp-mksys-file (string-append new-file-full-path ftp-vfs-file-spath)
                                            (ftp-user-login current-ftp-user) (ftp-user-group current-ftp-user)))
                          (let-values ([(in out) (net-connect host port)])
                            (print-connect)
                            (when restart-marker
                              (file-position fout restart-marker)
                              (set! restart-marker #f))
                            (let-values ([(reset-alarm kill-alarm)
                                          (alarm-clock 1 15
                                                       (λ()
                                                         ;(displayln "Auto kill working process!" log-output-port)
                                                         (custodian-shutdown-all current-process)))])
                              (let loop ([dat (read-bytes 1048576 in)])
                                (reset-alarm)
                                (unless (eof-object? dat)
                                  (write-bytes dat fout)
                                  (loop (read-bytes 1048576 in))))
                              (kill-alarm))
                            (flush-output fout)
                            (log-file-event new-file-full-path exists-mode)
                            (print-close)))
                        #:mode 'binary
                        #:exists exists-mode))
                    ;(displayln "Process complete!" log-output-port)
                    (custodian-shutdown-all current-process))))))
    
    (define (passive-store-file current-ftp-user new-file-full-path exists-mode)
      (parameterize ([current-custodian current-process])
        (thread (λ ()
                  (with-handlers ([any/c (λ (e) (print-abort))])
                    (call-with-output-file new-file-full-path
                      (λ (fout)
                        (unless (file-exists? (string-append new-file-full-path ftp-vfs-file-spath))
                          (ftp-mksys-file (string-append new-file-full-path ftp-vfs-file-spath)
                                          (ftp-user-login current-ftp-user) (ftp-user-group current-ftp-user)))
                        (let-values ([(in out) (net-accept pasv-listener)])
                          (print-connect)
                          (when restart-marker
                            (file-position fout restart-marker)
                            (set! restart-marker #f))
                          (let-values ([(reset-alarm kill-alarm)
                                        (alarm-clock 1 15
                                                     (λ()
                                                       ;(displayln "Auto kill working process!" log-output-port)
                                                       (custodian-shutdown-all current-process)))])
                            (let loop ([dat (read-bytes 1048576 in)])
                              (reset-alarm)
                              (unless (eof-object? dat)
                                (write-bytes dat fout)
                                (loop (read-bytes 1048576 in))))
                            (kill-alarm))
                          (flush-output fout)
                          (log-file-event new-file-full-path exists-mode)
                          (print-close)))
                      #:mode 'binary
                      #:exists exists-mode))
                  ;(displayln "Process complete!" log-output-port)
                  (custodian-shutdown-all current-process)))))
    
    (define-syntax (server-1-host stx)
      #'(ftp-server-params-server-1-host server-params))
    
    (define-syntax (default-locale-encoding stx)
      #'(ftp-server-params-default-locale-encoding server-params))
    
    (define-syntax (passive-1-ports-from stx)
      #'(passive-ports-from (ftp-server-params-passive-1-ports server-params)))
    
    (define-syntax (passive-2-ports-from stx)
      #'(passive-ports-from (ftp-server-params-passive-2-ports server-params)))
    
    ))

(define ftp-session%
  (class ftp-DTP%
    (inherit get-params
             print/encoding
             print-crlf/encoding
             print*-crlf/encoding
             string->bytes/encoding
             read-request
             real-path->ftp-path
             ftp-file-or-dir-full-info
             simplify-ftp-path
             ftp-file-name-safe?
             ftp-file-exists?
             ftp-file-allow-read?
             ftp-file-allow-write?
             ftp-dir-exists?
             ftp-dir-allow-read?
             ftp-dir-allow-write?
             ftp-dir-allow-execute?
             ftp-access-allow?
             ftp-file-or-dir-sysbytes/owner
             ftp-mkdir*
             ftp-mksys-file
             ftp-data-transfer
             ftp-store-file)
    
    (inherit-field server-params
                   current-server
                   ssl-server-context
                   ssl-client-context
                   current-process
                   active-host&port
                   passive-host&port
                   pasv-listener
                   restart-marker
                   DTP
                   representation-type
                   transfer-mode
                   file-structure)
    ;;
    ;; ---------- Public Definitions ----------
    ;;
    (init-field welcome-message
                [disable-commands null])
    ;;
    ;; ---------- Superclass Initialization ----------
    ;;
    (super-new [print-abort (λ() (print-crlf/encoding** 'TRANSFER-ABORTED))]
               [print-connect (λ() (print-crlf/encoding** 'OPEN-DATA-CONNECTION representation-type))]
               [print-close (λ() (print-crlf/encoding** 'TRANSFER-OK))]
               [print-ascii (λ(data out) (print/encoding *locale-encoding* data out))]
               [log-file-event (λ(new-file-full-path exists-mode)
                                 (print-log-event
                                  (format "~a data to file ~a"
                                          (if (eq? exists-mode 'append) "Append" "Store")
                                          (real-path->ftp-path new-file-full-path *root-dir*))))])
    ;;
    ;; ---------- Private Definitions ----------
    ;;
    (define *client-host* #f)
    (define *client-input-port* #f)
    (define *client-output-port* #f)
    (define *current-protocol* #f)
    (define *locale-encoding* default-locale-encoding)
    (define *user-id* #f)
    (define *user-logged* #f)
    (define *root-dir* default-root-dir)
    (define *current-dir* "/")
    (define *rename-path* #f)
    (define *mlst-features* (ftp-mlst-features #t #t #f))
    (define *lang-list* (let ([r (hash-ref server-responses 'SYNTAX-ERROR)])
                          (map car r)))
    (define *current-lang* (car *lang-list*))
    
    (define net-accept (if ssl-server-context ssl-accept tcp-accept))
    (define net-addresses (if ssl-server-context ssl-addresses tcp-addresses))
    (define net-close (if ssl-server-context ssl-close tcp-close))
    (define net-abandon-port (if ssl-server-context ssl-abandon-port tcp-abandon-port))
    (define *cmd-list* null)
    (define *cmd-voc* #f)
    
    ;;
    ;; ---------- Public Methods ----------
    ;;
    (define/public (handle-client-request listener transfer-wait-time)
      (let ([cust (make-custodian)])
        (with-handlers ([any/c (λ(e) (custodian-shutdown-all cust))])
          (parameterize ([current-custodian cust])
            (set!-values (*client-input-port* *client-output-port*) (net-accept listener))
            (let-values ([(server-host client-host) (net-addresses *client-input-port*)])
              (set! *client-host* client-host)
              (set! *current-protocol* (if (IPv4? server-host) '|1| '|2|))
              (thread (λ ()
                        ;(fprintf log-output-port "[~a] Accept connection!\n" client-host)
                        (accept-client-request (connect-shutdown transfer-wait-time cust))
                        ;(fprintf log-output-port "[~a] Connection close!\n" client-host)
                        (kill-current-ftp-process)
                        (custodian-shutdown-all cust))))))))
    ;;
    ;; ---------- Private Methods ----------
    ;;
    (define (connect-shutdown time connect-cust)
      (let-values ([(reset kill)
                    (alarm-clock 1 time
                                 (λ()
                                   ;(fprintf log-output-port "[~a] Auto connection close!\n" *client-host*)
                                   ;421 No-transfer-time exceeded. Closing control connection.
                                   (custodian-shutdown-all connect-cust)))])
        reset))
    
    (define (accept-client-request [reset-timer void])
      (with-handlers ([any/c  #|displayln|# void])
        (print-crlf/encoding** 'WELCOME welcome-message)
        ;(sleep 1)
        (let loop ([request (read-request *locale-encoding* *client-input-port*)])
          (unless (eof-object? request)
            (when request
              ;(printf "[~a] ~a\n" *client-host* request)
              (let ([cmd (string-upcase (car (regexp-match #rx"[^ ]+" request)))]
                    [params (get-params request)])
                (if *user-logged*
                    (let ([rec (hash-ref *cmd-voc* cmd #f)])
                      (if rec
                          ((car rec) params)
                          (print-crlf/encoding** 'CMD-NOT-IMPLEMENTED cmd)))
                    (case (string->symbol cmd)
                      ((USER) (USER-COMMAND params))
                      ((PASS) (PASS-COMMAND params))
                      ((QUIT) (QUIT-COMMAND params))
                      (else (print-crlf/encoding** 'PLEASE-LOGIN))))))
            (reset-timer)
            (sleep .005)
            (loop (read-request *locale-encoding* *client-input-port*))))))
    
    (define (USER-COMMAND params)
      (if params
          (let ([name params])
            (if (and (hash-ref ftp-users name #f)
                     (string=? (ftp-user-pass (hash-ref ftp-users name)) ""))
                (print-crlf/encoding** 'ANONYMOUS-LOGIN)
                (print-crlf/encoding** 'PASSW-REQUIRED name))
            (set! *user-id* name))
          (begin
            (print-crlf/encoding** 'SYNTAX-ERROR "")
            (set! *user-id* #f)))
      (set! *user-logged* #f))
    
    (define (PASS-COMMAND params)
      (sleep pass-sleep-sec)
      (let ([correct?
             (cond
               ((string? *user-id*)
                (let ([pass params])
                  (cond
                    ((not (hash-ref ftp-users *user-id* #f))
                     (print-log-event "Login incorrect.")
                     (print-crlf/encoding** 'LOGIN-INCORRECT)
                     #f)
                    ((string=? (ftp-user-pass (hash-ref ftp-users *user-id*))
                               "")
                     (print-log-event "User logged in.")
                     (print-crlf/encoding** 'ANONYMOUS-LOGGED)
                     #t)
                    ((and (hash-ref bad-auth-table *user-id* #f)
                          ((mcar (hash-ref bad-auth-table *user-id*)). >= . max-auth-attempts)
                          (<= ((current-seconds). - .(mcdr (hash-ref bad-auth-table *user-id*)))
                              bad-auth-sleep-sec))
                     (let ([pair (hash-ref bad-auth-table *user-id*)])
                       (set-mcar! pair (add1 (mcar pair)))
                       (set-mcdr! pair (current-seconds)))
                     (print-log-event "Login incorrect.")
                     (print-crlf/encoding** 'LOGIN-INCORRECT)
                     #f)
                    ((not pass)
                     (print-log-event "Login incorrect.")
                     (print-crlf/encoding** 'LOGIN-INCORRECT)
                     #f)
                    ((string=? (ftp-user-pass (hash-ref ftp-users *user-id*))
                               pass)
                     (when (hash-ref bad-auth-table *user-id* #f)
                       (hash-remove! bad-auth-table *user-id*))
                     (print-log-event "User logged in.")
                     (print-crlf/encoding** 'USER-LOGGED *user-id*)
                     #t)
                    (else
                     (if (hash-ref bad-auth-table *user-id* #f)
                         (let ([pair (hash-ref bad-auth-table *user-id*)])
                           (set-mcar! pair (add1 (mcar pair)))
                           (set-mcdr! pair (current-seconds)))
                         (hash-set! bad-auth-table *user-id* (mcons 1 (current-seconds))))
                     (print-log-event "Password incorrect.")
                     (print-crlf/encoding** 'LOGIN-INCORRECT)
                     #f))))
               (else
                (print-crlf/encoding** 'LOGIN-INCORRECT)
                #f))])
        (set! *user-logged* correct?)
        (when correct?
          (set! *root-dir* (ftp-user-root-dir (hash-ref ftp-users *user-id*))))))
    
    (define (REIN-COMMAND params)
      (if params
          (print-crlf/encoding** 'SYNTAX-ERROR "")
          (begin
            (set! *user-id* #f)
            (set! *user-logged* #f)
            (print-crlf/encoding** 'SERVICE-READY))))
    
    (define (QUIT-COMMAND params)
      (if params
          (print-crlf/encoding** 'SYNTAX-ERROR "")
          (begin
            (kill-current-ftp-process)
            (print-crlf/encoding** 'QUIT)
            (close-output-port *client-output-port*);ssl required
            (raise 'quit))))
    
    (define (PWD-COMMAND params)
      (if params
          (print-crlf/encoding** 'SYNTAX-ERROR "")
          (print-crlf/encoding** 'CURRENT-DIR *current-dir*)))
    
    (define (CDUP-COMMAND params)
      (if params
          (print-crlf/encoding** 'SYNTAX-ERROR "")
          (begin
            (set! *current-dir* (simplify-ftp-path *current-dir* 1))
            (print-crlf/encoding** 'CMD-SUCCESSFUL 250 "CDUP"))))
    
    (define (ABOR-COMMAND params)
      (if params
          (print-crlf/encoding** 'SYNTAX-ERROR "")
          (begin
            (kill-current-ftp-process)
            (print-crlf/encoding** 'ABORT))))
    
    (define (NOOP-COMMAND params)
      (if params
          (print-crlf/encoding** 'SYNTAX-ERROR "")
          (print-crlf/encoding** 'CMD-SUCCESSFUL 200 "NOOP")))
    
    (define (SYST-COMMAND params)
      (if params
          (print-crlf/encoding** 'SYNTAX-ERROR "")
          (print-crlf/encoding** 'SYSTEM)))
    
    (define (FEAT-COMMAND params)
      (if params
          (print-crlf/encoding** 'SYNTAX-ERROR "")
          (begin
            (print-crlf/encoding** 'FEAT-LIST)
            (print-crlf/encoding* " CLNT")
            (print-crlf/encoding* (string-append
                                   " LANG "
                                   (string-join (map (λ(l)
                                                       (string-append (symbol->string l)
                                                                      (if (eq? l *current-lang*) "*" "")))
                                                     *lang-list*)
                                                ";")))
            (print-crlf/encoding* " EPRT")
            (print-crlf/encoding* " EPSV")
            (print-crlf/encoding* " UTF8")
            (print-crlf/encoding* " REST STREAM")
            (print-crlf/encoding* " MLST size*;modify*;perm")
            (print-crlf/encoding* " MLSD")
            (print-crlf/encoding* " SIZE")
            (print-crlf/encoding* " MDTM")
            (print-crlf/encoding* " TVFS")
            (print-crlf/encoding** 'END 211))))
    
    (define (CLNT-COMMAND params)
      (if params
          (print-crlf/encoding** 'CLNT)
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    ; error!
    (define (PROT-COMMAND params)
      (if params
          (print-crlf/encoding* "200 Protection level set to P")
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    ; error!
    (define (PBSZ-COMMAND params)
      (if params
          (print-crlf/encoding* "200 PBSZ=0")
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    (define (PORT-COMMAND params)
      (with-handlers ([any/c (λ (e) (print-crlf/encoding** 'SYNTAX-ERROR ""))])
        (unless (regexp-match #rx"^[0-9]+,[0-9]+,[0-9]+,[0-9]+,[0-9]+,[0-9]+$" params)
          (raise 'error))
        (let* ([l (regexp-split #rx"," params)]
               [host (string-append (first l) "." (second l) "." (third l) "." (fourth l))]
               [port (((string->number (fifth l)). * . 256). + .(string->number (sixth l)))])
          (when (or (not (IPv4? host)) (negative? port) (port . > . #xffff)) (raise 'error))
          (set! DTP 'active)
          (set! active-host&port (ftp-host&port host port))
          (print-crlf/encoding** 'CMD-SUCCESSFUL 200 "PORT"))))
    
    (define (REST-COMMAND params)
      (with-handlers ([any/c (λ (e) (print-crlf/encoding** 'SYNTAX-ERROR ""))])
        (set! restart-marker (string->number (car (regexp-match #rx"^[0-9]+$" params))))
        (print-crlf/encoding** 'RESTART)))
    
    (define (ALLO-COMMAND params)
      (if (params . and .(regexp-match #rx"^[0-9]+$" params))
          (print-crlf/encoding** 'CMD-SUCCESSFUL 200 "ALLO")
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    (define (STAT-COMMAND params)
      (if params
          (begin
            (print-crlf/encoding** 'STATUS-LIST params)
            (DIR-LIST params #f #t)
            (print-crlf/encoding** 'END 213))
          (begin
            (print-crlf/encoding** 'STATUS-INFO-1)
            (print-crlf/encoding** 'STATUS-INFO-2 *client-host*)
            (print-crlf/encoding** 'STATUS-INFO-3 *user-id*)
            (print-crlf/encoding** 'STATUS-INFO-4 representation-type file-structure transfer-mode)
            ; Print status of the operation in progress?
            (print-crlf/encoding** 'END 211))))
    
    (define (LANG-COMMAND params)
      (if params
          (let ([lang (string->symbol (string-upcase params))])
            (if (memq lang *lang-list*)
                (begin
                  (set! *current-lang* lang)
                  (print-crlf/encoding** 'SET-CMD 200 "LANG" *current-lang*))
                (print-crlf/encoding** 'MISSING-PARAMS)))
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    (define (DIR-LIST params [short? #f][status #f])
      (local
        [(define (month->string m)
           (case m
             [(1) "Jan"][(2)  "Feb"][(3)  "Mar"][(4)  "Apr"]
             [(5) "May"][(6)  "Jun"][(7)  "Jul"][(8)  "Aug"]
             [(9) "Sep"][(10) "Oct"][(11) "Nov"][(12) "Dec"]))
         
         (define (date-time->string dte)
           (string-append (number->string (date-day dte)) " " (month->string (date-month dte)) " "
                          (if ((date-year (current-date)). = .(date-year dte))
                              (format "~a~d:~a~d"
                                      (if (< (date-hour dte) 10) "0" "")
                                      (date-hour dte)
                                      (if (< (date-minute dte) 10) "0" "")
                                      (date-minute dte))
                              (number->string (date-year dte)))))
         
         (define (read-ftp-sys-info ftp-path full-ftp-sys-file-spath)
           (let* ([info (ftp-file-or-dir-full-info full-ftp-sys-file-spath)]
                  [sysbytes (vector-ref info 0)])
             `(,(string (if (bitwise-bit-set? sysbytes 8) #\r #\-)
                        (if (bitwise-bit-set? sysbytes 7) #\w #\-)
                        (if (bitwise-bit-set? sysbytes 6) #\x #\-))
               ,(string (if (bitwise-bit-set? sysbytes 5) #\r #\-)
                        (if (bitwise-bit-set? sysbytes 4) #\w #\-)
                        (if (bitwise-bit-set? sysbytes 3) #\x #\-))
               ,(string (if (bitwise-bit-set? sysbytes 2) #\r #\-)
                        (if (bitwise-bit-set? sysbytes 1) #\w #\-)
                        (if (bitwise-bit-set? sysbytes 0) #\x #\-))
               ,(vector-ref info 1) ,(vector-ref info 2) ,ftp-path)))
         
         (define (read-ftp-dir-sys-info ftp-path spath)
           (let ([info (read-ftp-sys-info ftp-path (string-append spath ftp-vfs-dir-spath))])
             (string-append "d" (car info) (second info) (third info) " 2 " (fourth info) " " (fifth info))))
         
         (define (read-ftp-file-sys-info ftp-path spath)
           (let ([info (read-ftp-sys-info ftp-path (string-append spath ftp-vfs-file-spath))])
             (string-append "-" (car info) (second info) (third info) " 1 " (fourth info) " " (fifth info))))
         
         (define (dlst ftp-dir-name)
           (let* ([full-dir-name (string-append *root-dir* ftp-dir-name)]
                  [dirlist 
                   (if (ftp-dir-allow-read*? full-dir-name)
                       (string-append*
                        (map (λ (p)
                               (let* ([spath (path->string p)]
                                      [ftp-full-spath (string-append (simplify-ftp-path ftp-dir-name) "/" spath)]
                                      [full-spath (string-append full-dir-name "/" spath)])
                                 (if (or (ftp-file-exists? full-spath)
                                         (ftp-dir-exists? full-spath))
                                     (if short?
                                         (string-append spath "\n")
                                         (let ([d (seconds->date (file-or-directory-modify-seconds full-spath))])
                                           [string-append
                                            (if (ftp-dir-exists? full-spath)
                                                (format "~a~15F " (read-ftp-dir-sys-info ftp-full-spath full-spath) 0)
                                                (format "~a~15F "
                                                        (read-ftp-file-sys-info ftp-full-spath full-spath)
                                                        (file-size full-spath)))
                                            (date-time->string d) " " spath "\n"]))
                                     "")))
                             (directory-list full-dir-name)))
                       "")])
             (if status
                 (print-crlf/encoding* dirlist)
                 (ftp-data-transfer (case representation-type
                                      ((ASCII) dirlist)
                                      ((Image) (string->bytes/encoding *locale-encoding* dirlist)))))))]
        
        (let ([dir (if (and params (eq? (string-ref params 0) #\-))
                       (let ([d (regexp-match #px"[^-\\w][^ \t-]+.*" params)])
                         (and d (substring (car d) 1)))
                       params)])
          (cond
            ((not dir)
             (dlst *current-dir*))
            ((memq (string-ref dir 0) '(#\/ #\\))
             (if (and (ftp-dir-exists? (string-append *root-dir* dir))
                      (ftp-perm-allow? dir))
                 (if (ftp-dir-allow-execute*? (string-append *root-dir* dir))
                     (dlst dir)
                     (unless status
                       (print-crlf/encoding** 'PERM-DENIED)))
                 (unless status
                   (print-crlf/encoding** 'DIR-NOT-FOUND))))
            ((and (ftp-dir-exists? (string-append *root-dir* *current-dir* "/" dir))
                  (ftp-perm-allow? (string-append *current-dir* "/" dir)))
             (if (ftp-dir-allow-execute*? (string-append *root-dir* *current-dir* "/" dir))
                 (dlst (string-append *current-dir* "/" dir))
                 (unless status
                   (print-crlf/encoding** 'PERM-DENIED))))
            (else
             (unless status
               (print-crlf/encoding** 'DIR-NOT-FOUND)))))))
    
    (define (RETR-COMMAND params)
      (local [(define (fcopy full-path-file)
                (ftp-data-transfer full-path-file #t))]
        
        (cond
          ((not params)
           (print-crlf/encoding** 'SYNTAX-ERROR ""))
          ((and (memq (string-ref params 0) '(#\/ #\\))
                (ftp-file-exists? (string-append *root-dir* params)))
           (if (ftp-perm-allow? params)
               (if (ftp-file-allow-read*? (string-append *root-dir* params))
                   (fcopy (string-append *root-dir* params))
                   (print-crlf/encoding** 'PERM-DENIED))
               (print-crlf/encoding** 'FILE-NOT-FOUND)))
          ((ftp-file-exists? (string-append *root-dir* *current-dir* "/" params))
           (if (ftp-perm-allow? (string-append *current-dir* "/" params))
               (if (ftp-file-allow-read*? (string-append *root-dir* *current-dir* "/" params))
                   (fcopy (string-append *root-dir* *current-dir* "/" params))
                   (print-crlf/encoding** 'PERM-DENIED))
               (print-crlf/encoding** 'FILE-NOT-FOUND)))
          (else
           (print-crlf/encoding** 'FILE-NOT-FOUND)))))
    
    (define (TYPE-COMMAND params)
      (if params
          (case (string->symbol (string-upcase (car (regexp-split #rx" +" params))))
            ((A)
             (set! representation-type 'ASCII)
             (print-crlf/encoding** 'SET-CMD 200 "TYPE" representation-type))
            ((I)
             (set! representation-type 'Image)
             (print-crlf/encoding** 'SET-CMD 200 "TYPE" representation-type))
            ((E L)
             (print-crlf/encoding** 'MISSING-PARAMS))
            (else
             (print-crlf/encoding** 'UNSUPTYPE)))
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    (define (MODE-COMMAND params)
      (if params
          (case (string->symbol (string-upcase params))
            ((S)
             (print-crlf/encoding** 'SET-CMD 200 "MODE" transfer-mode))
            ((B C)
             (print-crlf/encoding** 'MISSING-PARAMS))
            (else
             (print-crlf/encoding** 'UNKNOWN-TYPE "MODE")))
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    (define (STRU-COMMAND params)
      (if params
          (case (string->symbol (string-upcase params))
            ((F)
             (print-crlf/encoding** 'SET-CMD 200 "FILE STRUCTURE" file-structure))
            ((R P)
             (print-crlf/encoding** 'MISSING-PARAMS))
            (else
             (print-crlf/encoding** 'UNKNOWN-TYPE "FILE STRUCTURE")))
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    (define (CWD-COMMAND params)
      (cond
        ((not params)
         (print-crlf/encoding** 'SYNTAX-ERROR ""))
        ((and (memq (string-ref params 0) '(#\/ #\\))
              (ftp-dir-exists? (string-append *root-dir* params)))
         (if (ftp-perm-allow? params)
             (begin
               (set! *current-dir* (simplify-ftp-path params))
               (print-crlf/encoding** 'CMD-SUCCESSFUL 250 "CWD"))
             (print-crlf/encoding** 'DIR-NOT-FOUND)))
        ((ftp-dir-exists? (string-append *root-dir* *current-dir* "/" params))
         (if (ftp-perm-allow? (string-append *current-dir* "/" params))
             (begin
               (set! *current-dir* (simplify-ftp-path (string-append *current-dir* "/" params)))
               (print-crlf/encoding** 'CMD-SUCCESSFUL 250 "CWD"))
             (print-crlf/encoding** 'DIR-NOT-FOUND)))
        (else
         (print-crlf/encoding** 'DIR-NOT-FOUND))))
    
    (define (MKD-COMMAND params)
      (local [(define (mkd ftp-parent-path dir-name user)
                (if (ftp-access-allow*? ftp-parent-path)
                    (let* ([full-parent-path (string-append *root-dir* ftp-parent-path)]
                           [sp (string-append full-parent-path "/" dir-name)])
                      (if (ftp-dir-allow-write*? full-parent-path)
                          (if (ftp-dir-exists? sp)
                              (print-crlf/encoding** 'DIR-EXIST)
                              (let* ([fpp (simplify-ftp-path ftp-parent-path)]
                                     [fp (string-append fpp (if (string=? fpp "/") "" "/") dir-name)])
                                (ftp-mkdir* sp (ftp-user-login user) (ftp-user-group user))
                                (print-log-event (format "Make directory ~a" fp))
                                (print-crlf/encoding** 'DIR-CREATED fp)))
                          (print-crlf/encoding** 'CREATE-DIR-PERM-DENIED)))
                    (print-crlf/encoding** 'CREATE-DIR-PERM-DENIED)))]
        
        (if params
            (let* ([path (if (memq (string-ref params (sub1 (string-length params))) '(#\/ #\\))
                             (car (regexp-match #rx".*[^/\\\\]+" params))
                             params)]
                   [dir-name (and (file-name-from-path path)
                                  (path->string (file-name-from-path path)))]
                   [parent-path (and dir-name (path-only path)
                                     (path->string (path-only path)))]
                   [user current-ftp-user])
              (cond
                ((not dir-name)
                 (print-crlf/encoding** 'CANT-CREATE-DIR))
                ((and (not parent-path)
                      (ftp-dir-exists? (string-append *root-dir* *current-dir* "/" dir-name)))
                 (if (ftp-access-allow*? *current-dir*)
                     (print-crlf/encoding** 'DIR-EXIST)
                     (print-crlf/encoding** 'CREATE-DIR-PERM-DENIED)))
                ((not parent-path)
                 (mkd *current-dir* dir-name user))
                ((memq (string-ref parent-path 0) '(#\/ #\\))
                 (if (ftp-dir-exists? (string-append *root-dir* parent-path))
                     (mkd parent-path dir-name user)
                     (print-crlf/encoding** 'CANT-CREATE-DIR)))
                ((ftp-dir-exists? (string-append *root-dir* *current-dir* "/" parent-path))
                 (mkd (string-append *current-dir* "/" parent-path) dir-name user))
                (else
                 (print-crlf/encoding** 'CANT-CREATE-DIR))))
            (print-crlf/encoding** 'SYNTAX-ERROR ""))))
    
    (define (RMD-COMMAND params)
      (local
        [(define (rmd ftp-path)
           (if (ftp-perm-allow? ftp-path)
               (let ([spath (string-append *root-dir* ftp-path)])
                 (if ((file-or-directory-identity spath). = .(file-or-directory-identity *root-dir*))
                     (print-crlf/encoding** 'DIR-NOT-FOUND)
                     (if (ftp-dir-allow-write*? (simplify-ftp-path spath 1))
                         (let ([lst (directory-list spath)])
                           (if (> (length lst) 1)
                               (print-crlf/encoding** 'DELDIR-NOT-EMPTY)
                               (with-handlers ([exn:fail:filesystem? (λ (e)
                                                                       (print-crlf/encoding* "550 System error."))])
                                 (delete-file (string-append spath ftp-vfs-dir-spath))
                                 (delete-directory spath)
                                 (print-log-event (format "Remove a directory ~a" (simplify-ftp-path ftp-path)))
                                 (print-crlf/encoding** 'CMD-SUCCESSFUL 250 "RMD"))))
                         (print-crlf/encoding** 'DELDIR-PERM-DENIED))))
               (print-crlf/encoding** 'DIR-NOT-FOUND)))]
        
        (cond
          ((and (memq (string-ref params 0) '(#\/ #\\))
                (ftp-dir-exists? (string-append *root-dir* params)))
           (rmd params))
          ((ftp-dir-exists? (string-append *root-dir* *current-dir* "/" params))
           (rmd (string-append *current-dir* "/" params)))
          (else
           (print-crlf/encoding** 'DIR-NOT-FOUND)))))
    
    (define (STORE-FILE params [exists-mode 'truncate])
      (local [(define (stor ftp-parent-path file-name)
                (if (ftp-access-allow*? ftp-parent-path)
                    (let ([real-path (string-append *root-dir* ftp-parent-path "/" file-name)])
                      (if (and (ftp-dir-allow-write*? (string-append *root-dir* ftp-parent-path))
                               (ftp-file-name-safe? real-path)
                               (or (not (ftp-file-exists? real-path))
                                   (ftp-file-allow-write*? real-path)))
                          (ftp-store-file current-ftp-user real-path exists-mode)
                          (print-crlf/encoding** 'STORE-FILE-PERM-DENIED)))
                    (print-crlf/encoding** 'CANT-STORE-FILE)))]
        
        (if params
            (let* ([file-name (and (file-name-from-path params)
                                   (path->string (file-name-from-path params)))]
                   [parent-path (and file-name (path-only params)
                                     (path->string (path-only params)))])
              (cond
                ((not file-name)
                 (print-crlf/encoding** 'CANT-STORE-FILE))
                ((not parent-path)
                 (stor *current-dir* file-name))
                ((and (memq (string-ref parent-path 0) '(#\/ #\\))
                      (ftp-dir-exists? (string-append *root-dir* parent-path)))
                 (stor parent-path file-name))
                ((ftp-dir-exists? (string-append *root-dir* *current-dir* "/" parent-path))
                 (stor (string-append *current-dir* "/" parent-path) file-name))
                (else
                 (print-crlf/encoding** 'CANT-STORE-FILE))))
            (print-crlf/encoding** 'SYNTAX-ERROR ""))))
    
    (define (STOU-FILE params)
      (cond
        (params
         (print-crlf/encoding** 'SYNTAX-ERROR ""))
        ((and (ftp-access-allow*? *current-dir*)
              (ftp-dir-allow-write*? (string-append *root-dir* *current-dir*)))
         (let* ([file-name (let loop ([fname (gensym "noname")])
                             (if (ftp-file-exists? (string-append *root-dir* *current-dir* "/" fname))
                                 (loop (gensym "noname"))
                                 fname))]
                [path (string-append *root-dir* *current-dir* "/" file-name)])
           (ftp-store-file current-ftp-user path 'truncate)))
        (else
         (print-crlf/encoding** 'STORE-FILE-PERM-DENIED))))
    
    (define (DELE-COMMAND params)
      (local
        [(define (dele ftp-path)
           (if (ftp-perm-allow? ftp-path)
               (let ([spath (string-append *root-dir* ftp-path)])
                 (if (ftp-dir-allow-write*? (simplify-ftp-path spath 1))
                     (with-handlers ([exn:fail:filesystem? (λ (e)
                                                             (print-crlf/encoding* "550 System error."))])
                       (delete-file (string-append spath ftp-vfs-file-spath))
                       (delete-file spath)
                       (print-log-event (format "Delete a file ~a" (simplify-ftp-path ftp-path)))
                       (print-crlf/encoding** 'CMD-SUCCESSFUL 250 "DELE"))
                     (print-crlf/encoding** 'DELFILE-PERM-DENIED)))
               (print-crlf/encoding** 'FILE-NOT-FOUND)))]
        
        (cond
          ((and (memq (string-ref params 0) '(#\/ #\\))
                (ftp-file-exists? (string-append *root-dir* params)))
           (dele params))
          ((ftp-file-exists? (string-append *root-dir* *current-dir* "/" params))
           (dele (string-append *current-dir* "/" params)))
          (else
           (print-crlf/encoding** 'FILE-NOT-FOUND)))))
    
    (define (SITE-COMMAND params)
      (local
        [(define (chmod permis path)
           (let* ([permis (if ((length permis). = . 4)
                              (cdr permis)
                              permis)]
                  [ch->num (λ (c)
                             (case c
                               [(#\7) 7][(#\6) 6][(#\5) 5][(#\4) 4]
                               [(#\3) 3][(#\2) 2][(#\1) 1][(#\0) 0]))]
                  [get-permis (λ (cl)
                                (+ (arithmetic-shift (ch->num (car cl)) 6)
                                   (arithmetic-shift (ch->num (second cl)) 3)
                                   (ch->num (third cl))))]
                  [fchmod 
                   (λ (ftp-spath target)
                     (if (ftp-perm-allow? ftp-spath)
                         (let* ([full-path (string-append *root-dir* ftp-spath)]
                                [owner (vector-ref (ftp-file-or-dir-sysbytes/owner (string-append full-path target)) 1)]
                                [user current-ftp-user])
                           (if (string=? owner (ftp-user-login user))
                               (begin
                                 (ftp-mksys-file (string-append full-path target)
                                                 (ftp-user-login user) (ftp-user-group user)
                                                 (get-permis permis))
                                 (print-crlf/encoding** 'CMD-SUCCESSFUL 200 "SITE CHMOD"))
                               (print-crlf/encoding** 'PERM-DENIED)))
                         (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)))])
             (cond
               ((memq (string-ref path 0) '(#\/ #\\))
                (cond
                  ((ftp-file-exists? (string-append *root-dir* path))
                   (fchmod path ftp-vfs-file-spath))
                  ((ftp-dir-exists? (string-append *root-dir* path))
                   (fchmod path ftp-vfs-dir-spath))
                  (else
                   (print-crlf/encoding** 'FILE-DIR-NOT-FOUND))))
               ((ftp-file-exists? (string-append *root-dir* *current-dir* "/" path))
                (fchmod (string-append *current-dir* "/" path) ftp-vfs-file-spath))
               ((ftp-dir-exists? (string-append *root-dir* *current-dir* "/" path))
                (fchmod (string-append *current-dir* "/" path) ftp-vfs-dir-spath))
               (else
                (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)))))]
        
        (if params
            (let ([cmd (string->symbol (string-upcase (car (regexp-match #rx"[^ ]+" params))))])
              (case cmd
                ((CHMOD)
                 (let* ([permis+tail (car (regexp-match #rx"[^ \tA-z]+.+" params))]
                        [permis (regexp-match #rx"[0-7]?[0-7][0-7][0-7]" permis+tail)])
                   (if permis
                       (let ([path (regexp-match #rx"[^ \t0-7]+.*" permis+tail)])
                         (if path
                             (chmod (string->list (car permis)) (car path))
                             (print-crlf/encoding** 'SYNTAX-ERROR "CHMOD:")))
                       (print-crlf/encoding** 'SYNTAX-ERROR "CHMOD:"))))
                (else (print-crlf/encoding** 'MISSING-PARAMS))))
            (print-crlf/encoding** 'SYNTAX-ERROR ""))))
    
    (define (MDTM-COMMAND params);; ???
      (if params
          (let ([mdtm 
                 (λ (ftp-spath)
                   (let ([full-path (string-append *root-dir* ftp-spath)])
                     (if (and (or (ftp-file-exists? full-path)
                                  (ftp-dir-exists? full-path))
                              (ftp-perm-allow? ftp-spath))
                         (print-crlf/encoding* (format "213 ~a"
                                                       (seconds->mdtm-time-format
                                                        (file-or-directory-modify-seconds full-path))))
                         (print-crlf/encoding** 'FILE-DIR-NOT-FOUND))))])
            (if (memq (string-ref params 0) '(#\/ #\\))
                (mdtm params)
                (mdtm (string-append *current-dir* "/" params))))
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    (define (SIZE-COMMAND params);; ???
      (if params
          (let ([size 
                 (λ (ftp-spath)
                   (let ([full-path (string-append *root-dir* ftp-spath)])
                     (if (and (ftp-file-exists? full-path)
                              (ftp-perm-allow? ftp-spath))
                         (print-crlf/encoding* (format "213 ~a" (file-size full-path)))
                         (print-crlf/encoding** 'FILE-NOT-FOUND))))])
            (if (memq (string-ref params 0) '(#\/ #\\))
                (size params)
                (size (string-append *current-dir* "/" params))))
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    (define (MLST-COMMAND params)
      (local [(define (mlst ftp-path)
                (if (and (ftp-perm-allow? ftp-path)
                         (ftp-dir-allow-read*? (string-append *root-dir* (simplify-ftp-path ftp-path 1))))
                    (begin
                      (print-crlf/encoding** 'MLST-LISTING)
                      (print-crlf/encoding* (string-append " " (mlst-info ftp-path)))
                      (print-crlf/encoding** 'END 250))
                    (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)))]
        (cond
          ((not params)
           (let ([path (string-append *root-dir* *current-dir*)])
             (if ((file-or-directory-identity path). = .(file-or-directory-identity *root-dir*))
                 (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)
                 (mlst *current-dir*))))
          ((memq (string-ref params 0) '(#\/ #\\))
           (let ([path (string-append *root-dir* params)])
             (cond
               ((ftp-file-exists? path)
                (mlst params))
               ((ftp-dir-exists? path)
                (if ((file-or-directory-identity path). = .(file-or-directory-identity *root-dir*))
                    (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)
                    (mlst params)))
               (else
                (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)))))
          (else
           (let ([path (string-append *root-dir* *current-dir* "/" params)])
             (cond
               ((ftp-file-exists? path)
                (mlst (string-append *current-dir* "/" params)))
               ((ftp-dir-exists? path)
                (if ((file-or-directory-identity path). = .(file-or-directory-identity *root-dir*))
                    (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)
                    (mlst (string-append *current-dir* "/" params))))
               (else
                (print-crlf/encoding** 'FILE-DIR-NOT-FOUND))))))))
    
    (define (MLSD-COMMAND params)
      (local [(define (mlsd ftp-path)
                (if (ftp-perm-allow? ftp-path)
                    (if (ftp-dir-allow-execute*? (string-append *root-dir* ftp-path))
                        (let* ([path (string-append *root-dir* ftp-path)]
                               [dirlist 
                                (if (ftp-dir-allow-read*? path)
                                    (string-append*
                                     (map (λ (p)
                                            (let* ([spath (path->string p)]
                                                   [full-spath (string-append path "/" spath)])
                                              (if (or (ftp-file-exists? full-spath)
                                                      (ftp-dir-exists? full-spath))
                                                  (string-append (mlst-info (string-append ftp-path "/" spath) #f)
                                                                 "\n")
                                                  "")))
                                          (directory-list path)))
                                    "")])
                          (ftp-data-transfer (case representation-type
                                               ((ASCII) dirlist)
                                               ((Image) (string->bytes/encoding *locale-encoding* dirlist)))))
                        (print-crlf/encoding** 'PERM-DENIED))
                    (print-crlf/encoding** 'DIR-NOT-FOUND)))]
        (cond
          ((not params)
           (mlsd *current-dir*))
          ((memq (string-ref params 0) '(#\/ #\\))
           (let ([path (string-append *root-dir* params)])
             (if (ftp-dir-exists? path)
                 (mlsd params)
                 (print-crlf/encoding** 'DIR-NOT-FOUND))))
          (else
           (let ([path (string-append *root-dir* *current-dir* "/" params)])
             (if (ftp-dir-exists? path)
                 (mlsd (string-append *current-dir* "/" params))
                 (print-crlf/encoding** 'DIR-NOT-FOUND)))))))
    
    (define (OPTS-COMMAND params)
      (if params
          (let ([cmd (string->symbol (string-upcase (car (regexp-match #rx"[^ ]+" params))))])
            (case cmd
              ((UTF8)
               (let ([mode (regexp-match #rx"[^ \t]+.+" (substring params 4))])
                 (if mode
                     (case (string->symbol (string-upcase (car mode)))
                       ((ON)
                        (set! *locale-encoding* "UTF-8")
                        (print-crlf/encoding** 'UTF8-ON))
                       ((OFF)
                        (set! *locale-encoding* default-locale-encoding)
                        (print-crlf/encoding** 'UTF8-OFF))
                       (else
                        (print-crlf/encoding** 'SYNTAX-ERROR "UTF8:")))
                     (print-crlf/encoding** 'SYNTAX-ERROR "UTF8:"))))
              ((MLST)
               (let ([modes (regexp-match #rx"[^ \t]+.+" (substring params 4))])
                 (if modes
                     (let ([mlst (map string->symbol
                                      (filter (λ (s) (not (string=? s "")))
                                              (regexp-split #rx"[; \t]+" (string-upcase (car modes)))))])
                       (if (andmap (λ (mode) (member mode '(SIZE MODIFY PERM))) mlst)
                           (let ([features *mlst-features*])
                             (for-each (λ (mode)
                                         (case mode
                                           ((SIZE) (set-ftp-mlst-features-size?! features #t))
                                           ((MODIFY) (set-ftp-mlst-features-modify?! features #t))
                                           ((PERM) (set-ftp-mlst-features-perm?! features #t))))
                                       mlst)
                             (print-crlf/encoding** 'MLST-ON))
                           (print-crlf/encoding** 'SYNTAX-ERROR "MLST:")))
                     (print-crlf/encoding** 'SYNTAX-ERROR "MLST:"))))
              (else (print-crlf/encoding** 'SYNTAX-ERROR ""))))
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    (define (RNFR-COMMAND params)
      (if params
          (let ([path1 (string-append *root-dir* params)]
                [path2 (string-append *root-dir* *current-dir* "/" params)])
            (cond
              ((memq (string-ref params 0) '(#\/ #\\))
               (cond
                 ((ftp-file-exists? path1)
                  (if (ftp-perm-allow? params)
                      (if (ftp-dir-allow-write*? (simplify-ftp-path path1 1))
                          (begin
                            (set! *rename-path* path1)
                            (print-crlf/encoding** 'RENAME-OK))
                          (print-crlf/encoding** 'PERM-DENIED))
                      (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)))
                 ((ftp-dir-exists? path1)
                  (if (ftp-perm-allow? params)
                      (if (ftp-dir-allow-write*? (simplify-ftp-path path1 1))
                          (begin
                            (set! *rename-path* path1)
                            (print-crlf/encoding** 'RENAME-OK))
                          (print-crlf/encoding** 'PERM-DENIED))
                      (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)))
                 (else
                  (print-crlf/encoding** 'FILE-DIR-NOT-FOUND))))
              ((ftp-file-exists? path2)
               (if (ftp-perm-allow? (string-append *current-dir* "/" params))
                   (if (ftp-dir-allow-write*? (simplify-ftp-path path2 1))
                       (begin
                         (set! *rename-path* path2)
                         (print-crlf/encoding** 'RENAME-OK))
                       (print-crlf/encoding** 'PERM-DENIED))
                   (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)))
              ((ftp-dir-exists? path2)
               (if (ftp-perm-allow? (string-append *current-dir* "/" params))
                   (if (ftp-dir-allow-write*? (simplify-ftp-path path2 1))
                       (begin
                         (set! *rename-path* path2)
                         (print-crlf/encoding** 'RENAME-OK))
                       (print-crlf/encoding** 'PERM-DENIED))
                   (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)))
              (else
               (print-crlf/encoding** 'FILE-DIR-NOT-FOUND))))
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    (define (RNTO-COMMAND params)
      (local [(define (move file? old-path ftp-parent-path name user)
                (if (ftp-access-allow*? ftp-parent-path)
                    (let ([new-path (string-append *root-dir* ftp-parent-path "/" name)])
                      (if (ftp-dir-allow-write*? (string-append *root-dir* ftp-parent-path))
                          (if (if file?
                                  (ftp-file-exists? new-path)
                                  (ftp-dir-exists? new-path))
                              (print-crlf/encoding** 'CANT-RENAME-EXIST)
                              (with-handlers ([exn:fail:filesystem?
                                               (λ (e) (print-crlf/encoding** 'CANT-RENAME))])
                                (when file?
                                  (rename-file-or-directory (string-append old-path ftp-vfs-file-spath)
                                                            (string-append new-path ftp-vfs-file-spath)))
                                (rename-file-or-directory old-path new-path)
                                (print-log-event (format "Rename the file or directory from ~a to ~a"
                                                         (real-path->ftp-path old-path *root-dir*)
                                                         (real-path->ftp-path new-path *root-dir*)))
                                (print-crlf/encoding** 'CMD-SUCCESSFUL 250 "RNTO")))
                          (print-crlf/encoding** 'RENAME-PERM-DENIED)))
                    (print-crlf/encoding** 'CANT-RENAME)))]
        
        (if params
            (if *rename-path*
                (let* ([old-path *rename-path*]
                       [old-file? (ftp-file-exists? old-path)]
                       [new-dir? (memq (string-ref params (sub1 (string-length params))) '(#\/ #\\))]
                       [path (if new-dir?
                                 (regexp-match #rx".*[^/\\\\]+" params)
                                 params)]
                       [name (and (file-name-from-path path)
                                  (path->string (file-name-from-path path)))]
                       [parent-path (and name (path-only path)
                                         (path->string (path-only path)))]
                       [user current-ftp-user]
                       [curr-dir *current-dir*])
                  (cond
                    ((or (not name)
                         (and old-file? new-dir?))
                     (print-crlf/encoding** 'CANT-RENAME))
                    ((and (not parent-path)
                          (if old-file?
                              (ftp-file-exists? (string-append *root-dir* curr-dir "/" name))
                              (ftp-dir-exists? (string-append *root-dir* curr-dir "/" name))))
                     (print-crlf/encoding** 'CANT-RENAME-EXIST))
                    ((not parent-path)
                     (move old-file? old-path curr-dir name user))
                    ((and (memq (string-ref parent-path 0) '(#\/ #\\))
                          (ftp-dir-exists? (string-append *root-dir* parent-path)))
                     (move old-file? old-path parent-path name user))
                    ((ftp-dir-exists? (string-append *root-dir* curr-dir "/" parent-path))
                     (move old-file? old-path (string-append curr-dir "/" parent-path) name user))
                    (else
                     (print-crlf/encoding** 'CANT-RENAME)))
                  (set! *rename-path* #f))
                (print-crlf/encoding** 'RENAME-PERM-DENIED))
            (print-crlf/encoding** 'SYNTAX-ERROR ""))))
    
    (define (PASV-COMMAND params)
      (if params
          (print-crlf/encoding** 'SYNTAX-ERROR "")
          (if (eq? *current-protocol* '|1|)
              (if (= current-server 1)
                  (let*-values ([(psv-port) (+ passive-1-ports-from
                                               (random (- passive-1-ports-to passive-1-ports-from -1)
                                                       random-gen))]
                                [(h1 h2 h3 h4) (apply values (regexp-split #rx"\\." passive-1-host))]
                                [(p1 p2) (quotient/remainder psv-port 256)])
                    (set! DTP 'passive)
                    (set! passive-host&port (ftp-host&port passive-1-host psv-port))
                    (kill-current-ftp-process)
                    (set! current-process (make-custodian))
                    (parameterize ([current-custodian current-process])
                      (set! pasv-listener (tcp-listen psv-port 1 #t server-1-host)))
                    (print-crlf/encoding** 'PASV h1 h2 h3 h4 p1 p2))
                  (let*-values ([(psv-port) (+ passive-2-ports-from
                                               (random (- passive-2-ports-to passive-2-ports-from -1)
                                                       random-gen))]
                                [(h1 h2 h3 h4) (apply values (regexp-split #rx"\\." passive-2-host))]
                                [(p1 p2) (quotient/remainder psv-port 256)])
                    (set! DTP 'passive)
                    (set! passive-host&port (ftp-host&port passive-2-host psv-port))
                    (kill-current-ftp-process)
                    (set! current-process (make-custodian))
                    (parameterize ([current-custodian current-process])
                      (set! pasv-listener (tcp-listen psv-port 1 #t server-2-host)))
                    (print-crlf/encoding** 'PASV h1 h2 h3 h4 p1 p2)))
              (print-crlf/encoding** 'BAD-PROTOCOL))))
    
    (define (EPRT-COMMAND params)
      (if params
          (with-handlers ([any/c (λ (e) (print-crlf/encoding** 'SYNTAX-ERROR "EPRT:"))])
            (let*-values ([(t1 net-prt ip tcp-port t2) (apply values (regexp-split #rx"\\|" params))]
                          [(prt port) (values (string->number net-prt) (string->number tcp-port))])
              (unless (and (string=? t1 t2 "")
                           (or (and (= prt 1) (IPv4? ip))
                               (and (= prt 2) (IPv6? ip)))
                           (port-number? port))
                (raise 'syntax))
              (set! DTP 'active)
              (set! active-host&port (ftp-host&port ip port))
              (print-crlf/encoding** 'CMD-SUCCESSFUL 200 "EPRT")))
          (print-crlf/encoding** 'SYNTAX-ERROR "EPRT:")))
    
    (define (EPSV-COMMAND params)
      (local [(define (set-psv)
                (set! DTP 'passive)
                (case current-server
                  ((1)
                   (let ([psv-port (+ passive-1-ports-from
                                      (random (- passive-1-ports-to passive-1-ports-from -1)
                                              random-gen))])
                     (set! passive-host&port (ftp-host&port passive-1-host psv-port))
                     (kill-current-ftp-process)
                     (set! current-process (make-custodian))
                     (parameterize ([current-custodian current-process])
                       (set! pasv-listener (tcp-listen psv-port 1 #t server-1-host)))
                     (print-crlf/encoding** 'EPSV psv-port)))
                  ((2)
                   (let ([psv-port (+ passive-2-ports-from
                                      (random (- passive-2-ports-to passive-2-ports-from -1)
                                              random-gen))])
                     (set! passive-host&port (ftp-host&port passive-2-host psv-port))
                     (kill-current-ftp-process)
                     (set! current-process (make-custodian))
                     (parameterize ([current-custodian current-process])
                       (set! pasv-listener (tcp-listen psv-port 1 #t server-2-host)))
                     (print-crlf/encoding** 'EPSV psv-port)))))
              (define (epsv-1)
                (if (eq? *current-protocol* '|1|)
                    (set-psv)
                    (print-crlf/encoding** 'BAD-PROTOCOL)))
              (define (epsv-2)
                (if (eq? *current-protocol* '|2|)
                    (set-psv)
                    (print-crlf/encoding** 'BAD-PROTOCOL)))
              (define (epsv)
                (if (eq? *current-protocol* '|1|) (epsv-1) (epsv-2)))]
        (if params
            (with-handlers ([any/c (λ (e) (print-crlf/encoding** 'SYNTAX-ERROR "EPSV:"))])
              (case (string->symbol (string-upcase (car (regexp-match #rx"1|2|[aA][lL][lL]" params))))
                [(|1|) (epsv-1)]
                [(|2|) (epsv-2)]
                [(ALL) (epsv)]))
            (epsv))))
    
    (define (HELP-COMMAND params)
      (if params
          (with-handlers ([any/c (λ (e) (print-crlf/encoding** 'UNKNOWN-CMD params))])
            (print-crlf/encoding** 'HELP (cdr (hash-ref *cmd-voc* (string-upcase params)))))
          (begin
            (print-crlf/encoding** 'HELP-LISTING)
            (for-each (λ (rec) (print-crlf/encoding* (format " ~a" (car rec)))) *cmd-list*)
            (print-crlf/encoding** 'END 214))))
    
    
    (define-syntax (bad-auth-table stx)
      #'(ftp-server-params-bad-auth server-params))
    
    (define-syntax (server-1-host stx)
      #'(ftp-server-params-server-1-host server-params))
    
    (define-syntax (server-2-host stx)
      #'(ftp-server-params-server-2-host server-params))
    
    (define-syntax (passive-1-host stx)
      #'server-1-host)
    
    (define-syntax (passive-2-host stx)
      #'server-2-host)
    
    (define-syntax (passive-1-ports-from stx)
      #'(passive-ports-from (ftp-server-params-passive-1-ports server-params)))
    
    (define-syntax (passive-1-ports-to stx)
      #'(passive-ports-to (ftp-server-params-passive-1-ports server-params)))
    
    (define-syntax (passive-2-ports-from stx)
      #'(passive-ports-from (ftp-server-params-passive-2-ports server-params)))
    
    (define-syntax (passive-2-ports-to stx)
      #'(passive-ports-to (ftp-server-params-passive-2-ports server-params)))
    
    (define-syntax (default-root-dir stx)
      #'(ftp-server-params-default-root-dir server-params))
    
    (define-syntax (default-locale-encoding stx)
      #'(ftp-server-params-default-locale-encoding server-params))
    
    (define-syntax (ftp-users stx)
      #'(ftp-server-params-ftp-users server-params))
    
    (define-syntax (ftp-groups stx)
      #'(ftp-server-params-ftp-groups server-params))
    
    (define-syntax (log-output-port stx)
      #'(ftp-server-params-log-output-port server-params))
    
    (define-syntax (server-responses stx)
      #'(ftp-server-params-server-responses server-params))
    
    (define-syntax (current-ftp-user stx)
      #'(hash-ref ftp-users *user-id*))
    
    (define-syntax (kill-current-ftp-process stx)
      #'(custodian-shutdown-all current-process))
    
    (define-syntax (bad-auth-sleep-sec stx)
      #'(ftp-server-params-bad-auth-sleep server-params))
    
    (define-syntax (max-auth-attempts stx)
      #'(ftp-server-params-max-auth-attempts server-params))
    
    (define-syntax (pass-sleep-sec stx)
      #'(ftp-server-params-pass-sleep server-params))
    
    (define-syntax-rule (print-crlf/encoding* txt)
      (print-crlf/encoding *locale-encoding* txt *client-output-port*))
    
    (define-syntax-rule (print-crlf/encoding** tag ...)
      (print*-crlf/encoding *locale-encoding* *client-output-port* *current-lang* server-responses tag ...))
    
    (define-syntax-rule (print-log-event msg ...)
      (short-print-log-event log-output-port *client-host* *user-id* msg ...))
    
    (define-syntax (ftp-access-allow*? stx)
      (syntax-case stx ()
        [(_ ftp-full-spath)
         #'(ftp-access-allow? *root-dir* ftp-full-spath current-ftp-user ftp-groups)]
        [(_ ftp-full-spath drop)
         #'(ftp-access-allow? *root-dir* ftp-full-spath current-ftp-user ftp-groups drop)]))
    
    (define-syntax-rule (ftp-perm-allow? ftp-full-spath)
      (ftp-access-allow*? ftp-full-spath 1))
    
    (define-syntax-rule (ftp-file-allow-read*? ftp-full-spath)
      (ftp-file-allow-read? ftp-full-spath current-ftp-user ftp-groups))
    
    (define-syntax-rule (ftp-file-allow-write*? ftp-full-spath)
      (ftp-file-allow-write? ftp-full-spath current-ftp-user ftp-groups))
    
    (define-syntax-rule (ftp-dir-allow-read*? ftp-full-spath)
      (ftp-dir-allow-read? ftp-full-spath current-ftp-user ftp-groups))
    
    (define-syntax-rule (ftp-dir-allow-write*? ftp-full-spath)
      (ftp-dir-allow-write? ftp-full-spath current-ftp-user ftp-groups))
    
    (define-syntax-rule (ftp-dir-allow-execute*? ftp-full-spath)
      (ftp-dir-allow-execute? ftp-full-spath current-ftp-user ftp-groups))
    
    (define (seconds->mdtm-time-format seconds)
      (let ([dte (seconds->date (- seconds ftp-date-zone-offset))])
        (format "~a~a~a~a~a~a~a~a~a~a~a"
                (date-year dte)
                (if (< (date-month dte) 10) "0" "")
                (date-month dte)
                (if (< (date-day dte) 10) "0" "")
                (date-day dte)
                (if (< (date-hour dte) 10) "0" "")
                (date-hour dte)
                (if (< (date-minute dte) 10) "0" "")
                (date-minute dte)
                (if (< (date-second dte) 10) "0" "")
                (date-second dte))))
    
    (define (mlst-info ftp-path [full-path? #t])
      (let* ([ftp-path (simplify-ftp-path ftp-path)]
             [path (string-append *root-dir* ftp-path)]
             [name (path->string (file-name-from-path ftp-path))]
             [file? (file-exists? path)]
             [info (ftp-file-or-dir-full-info (string-append path
                                                             (if file?
                                                                 ftp-vfs-file-spath
                                                                 ftp-vfs-dir-spath)))]
             [sysbytes (vector-ref info 0)]
             [user current-ftp-user]
             [user-name (ftp-user-login user)]
             [group (ftp-user-group user)]
             [features *mlst-features*])
        (string-append "Type=" (if file? "file" "dir") ";"
                       (if (and file? (ftp-mlst-features-size? features))
                           (format "Size=~d;" (file-size path))
                           "")
                       (if (ftp-mlst-features-modify? features)
                           (format "Modify=~a;" (seconds->mdtm-time-format (file-or-directory-modify-seconds path)))
                           "")
                       (if (ftp-mlst-features-perm? features)
                           (format "Perm=~a;"
                                   (cond
                                     ((string=? (vector-ref info 1) user-name)
                                      (string-append (if (bitwise-bit-set? sysbytes 8) (if file? "r" "el") "")
                                                     (if (bitwise-bit-set? sysbytes 7) (if file? "waf" "cmf") "")))
                                     ((string=? (vector-ref info 2) group)
                                      (string-append (if (bitwise-bit-set? sysbytes 5) (if file? "r" "el") "")
                                                     (if (bitwise-bit-set? sysbytes 4) (if file? "waf" "cmf") "")))
                                     (else
                                      (string-append (if (bitwise-bit-set? sysbytes 2) (if file? "r" "el") "")
                                                     (if (bitwise-bit-set? sysbytes 1) (if file? "waf" "cmf") "")))))
                           "")
                       " "
                       (if full-path? ftp-path name))))
    
    (define (init)
      (set! *cmd-list*
            (remove*
             (map symbol->string disable-commands)
             `(("ABOR" ,ABOR-COMMAND . "ABOR")
               ("ALLO" ,ALLO-COMMAND . "ALLO <SP> <decimal-integer>")
               ("APPE" ,(λ (params) (STORE-FILE params 'append)) . "APPE <SP> <pathname>")
               ("CDUP" ,CDUP-COMMAND . "CDUP")
               ("CLNT" ,CLNT-COMMAND . "CLNT <SP> <client-name>")
               ("CWD"  ,CWD-COMMAND . "CWD <SP> <pathname>")
               ("DELE" ,DELE-COMMAND . "DELE <SP> <pathname>")
               ("EPRT" ,EPRT-COMMAND . "EPRT <SP> <d> <address-family> <d> <ip-addr> <d> <port> <d>")
               ("EPSV" ,EPSV-COMMAND . "EPSV [<SP> (<address-family> | ALL)]")
               ("FEAT" ,FEAT-COMMAND . "FEAT")
               ("HELP" ,HELP-COMMAND . "HELP [<SP> <string>]")
               ("LANG" ,LANG-COMMAND . "LANG <SP> <lang-tag>")
               ("LIST" ,(λ (params) (DIR-LIST params)) . "LIST [<SP> <pathname>]")
               ("MDTM" ,MDTM-COMMAND . "MDTM <SP> <pathname>")
               ("MKD"  ,MKD-COMMAND . "MKD <SP> <pathname>")
               ("MLSD" ,MLSD-COMMAND . "MLSD [<SP> <pathname>]")
               ("MLST" ,MLST-COMMAND . "MLST [<SP> <pathname>]")
               ("MODE" ,MODE-COMMAND . "MODE <SP> <mode-code>")
               ("NLST" ,(λ (params) (DIR-LIST params #t)) . "NLST [<SP> <pathname>]")
               ("NOOP" ,NOOP-COMMAND . "NOOP")
               ("OPTS" ,OPTS-COMMAND . "OPTS <SP> <command-name> [<SP> <command-options>]")
               ("PASS" ,PASS-COMMAND . "PASS <SP> <password>")
               ("PASV" ,PASV-COMMAND . "PASV")
               ("PBSZ" ,PBSZ-COMMAND . "PBSZ <SP> <num>") ; error!
               ("PORT" ,PORT-COMMAND . "PORT <SP> <host-port>")
               ("PROT" ,PROT-COMMAND . "PROT <SP> <code>") ; error!
               ("PWD"  ,PWD-COMMAND . "PWD")
               ("QUIT" ,QUIT-COMMAND . "QUIT")
               ("REIN" ,REIN-COMMAND . "REIN")
               ("REST" ,REST-COMMAND . "REST <SP> <marker>")
               ("RETR" ,RETR-COMMAND . "RETR <SP> <pathname>")
               ("RMD"  ,RMD-COMMAND . "RMD <SP> <pathname>")
               ("RNFR" ,RNFR-COMMAND . "RNFR <SP> <pathname>")
               ("RNTO" ,RNTO-COMMAND . "RNTO <SP> <pathname>")
               ("SITE" ,SITE-COMMAND . "SITE <SP> <string>")
               ("SIZE" ,SIZE-COMMAND . "SIZE <SP> <pathname>")
               ("STAT" ,STAT-COMMAND . "STAT [<SP> <pathname>]")
               ("STOR" ,STORE-FILE . "STOR <SP> <pathname>")
               ("STOU" ,STOU-FILE . "STOU")
               ("STRU" ,STRU-COMMAND . "STRU <SP> <structure-code>")
               ("SYST" ,SYST-COMMAND . "SYST")
               ("TYPE" ,TYPE-COMMAND . "TYPE <SP> <type-code>")
               ("USER" ,USER-COMMAND . "USER <SP> <username>")
               ("XCUP" ,CDUP-COMMAND . "XCUP")
               ("XCWD" ,CWD-COMMAND . "XCWD <SP> <pathname>")
               ("XMKD" ,MKD-COMMAND . "XMKD <SP> <pathname>")
               ("XPWD" ,PWD-COMMAND . "XPWD")
               ("XRMD" ,RMD-COMMAND . "XRMD <SP> <pathname>"))
             string-ci=?))
      (set! *cmd-voc* (make-hash *cmd-list*)))
    
    (init)))

(define host/c (or/c host-string? not))
(define ssl-protocol/c (or/c ssl-protocol? not))
(define not-null-string/c (and/c string? (λ(dir) (not (string=? dir "")))))
(define ssl-certificate/c (or/c path-string? not))

(define/contract ftp-server%
  (class/c (init-field [welcome-message         string?]
                       
                       [server-1-host           host/c]
                       [server-1-port           port-number?]
                       [server-1-encryption     ssl-protocol/c]
                       [server-1-certificate    ssl-certificate/c]
                       
                       [server-2-host           host/c]
                       [server-2-port           port-number?]
                       [server-2-encryption     ssl-protocol/c]
                       [server-2-certificate    ssl-certificate/c]
                       
                       [max-allow-wait          exact-nonnegative-integer?]
                       [transfer-wait-time      exact-nonnegative-integer?]
                       
                       [bad-auth-sleep-sec      exact-nonnegative-integer?]
                       [max-auth-attempts       exact-nonnegative-integer?]
                       [pass-sleep-sec          exact-nonnegative-integer?]
                       
                       [disable-ftp-commands    (listof symbol?)]
                       
                       [passive-1-ports         passive-ports?]
                       [passive-2-ports         passive-ports?]
                       
                       [default-root-dir        not-null-string/c]
                       [default-locale-encoding string?]
                       [log-output-port         output-port?])
           [add-ftp-user (string?
                          not-null-string/c string?
                          not-null-string/c (non-empty-listof not-null-string/c) not-null-string/c . ->m . void?)]
           [add-ftp-group (not-null-string/c (listof not-null-string/c) . ->m . void?)])
  
  (class (ftp-utils% (ftp-vfs% object%))
    (super-new)
    (inherit get-params*
             ftp-dir-exists?
             ftp-mkdir*)
    ;;
    ;; ---------- Public Definitions ----------
    ;;
    (init-field [welcome-message         "Racket FTP Server!"]
                
                [server-1-host           "127.0.0.1"]
                [server-1-port           21]
                [server-1-encryption     #f]
                [server-1-certificate    #f]
                
                [server-2-host           #f]
                [server-2-port           #f]
                [server-2-encryption     #f]
                [server-2-certificate    #f]
                
                [max-allow-wait          25]
                [transfer-wait-time      120]
                
                [bad-auth-sleep-sec      60]
                [max-auth-attempts       5]
                [pass-sleep-sec          0]
                
                [disable-ftp-commands    null]
                
                [passive-1-ports         (make-passive-ports 40000 40999)]
                [passive-2-ports         (make-passive-ports 40000 40999)]
                
                [default-root-dir        "ftp-dir"]
                [default-locale-encoding "UTF-8"]
                [log-output-port         (current-output-port)])
    ;;
    ;; ---------- Private Definitions ----------
    ;;
    (define server-params #f)
    (define state 'stopped)
    (define server-custodian #f)
    (define server-ftp-users (make-hash))
    (define server-ftp-groups (make-hash))
    (define server-1-thread #f)
    (define server-2-thread #f)
    ;;
    ;; ---------- Public Methods ----------
    ;;
    (define/public (add-ftp-user full-name login pass group home-dirs [root-dir default-root-dir])
      (let ([root-dir (get-params* #rx".+" root-dir)])
        (hash-set! server-ftp-users login (ftp-user full-name login pass group home-dirs root-dir))
        (unless (ftp-dir-exists? root-dir)
          (ftp-mkdir* root-dir login group))
        (for-each (λ (home-dir)
                    (unless (ftp-dir-exists? (string-append root-dir home-dir))
                      (let ([dirs (filter (λ (s) (not (string=? s "")))
                                          (regexp-split #rx"[/\\\\]+" home-dir))]
                            [curr-dir ""])
                        (unless (zero? (length dirs))
                          (for-each (λ (p)
                                      (unless (ftp-dir-exists? (string-append root-dir curr-dir "/" p))
                                        (ftp-mkdir* (string-append root-dir curr-dir "/" p)
                                                    login group))
                                      (set! curr-dir (string-append curr-dir "/" p)))
                                    (drop-right dirs 1)))
                        (ftp-mkdir* (string-append root-dir home-dir) login group))))
                  home-dirs)))
    
    (define/public (add-ftp-group name users)
      (hash-set! server-ftp-groups name (make-hash (map (λ(u) (cons u #t)) users))))
    
    (define/public (clear-ftp-users)
      (set! server-ftp-users (make-hash)))
    
    (define/public (clear-ftp-groups)
      (set! server-ftp-groups (make-hash)))
    
    (define/public (start)
      (when (eq? state 'stopped)
        (set! server-custodian (make-custodian))
        (set! server-params (ftp-server-params passive-1-ports
                                               passive-2-ports
                                               server-1-host
                                               server-2-host
                                               default-server-responses
                                               default-root-dir
                                               default-locale-encoding
                                               log-output-port
                                               server-ftp-users   ;ftp-users
                                               server-ftp-groups  ;ftp-groups
                                               (make-hash)        ;bad-auth
                                               bad-auth-sleep-sec
                                               max-auth-attempts
                                               pass-sleep-sec))
        (unless (ftp-dir-exists? default-root-dir)
          (ftp-mkdir* default-root-dir))
        (parameterize ([current-custodian server-custodian])
          (when (and server-1-host server-1-port)
            (let* ([ssl-server-ctx
                    (and/exc server-1-encryption server-1-certificate
                             (let ([ctx (ssl-make-server-context server-1-encryption)])
                               (ssl-load-certificate-chain! ctx server-1-certificate default-locale-encoding)
                               (ssl-load-private-key! ctx server-1-certificate #t #f default-locale-encoding)
                               ctx))]
                   [ssl-client-ctx
                    (and/exc server-1-encryption server-1-certificate
                             (let ([ctx (ssl-make-client-context server-1-encryption)])
                               (ssl-load-certificate-chain! ctx server-1-certificate default-locale-encoding)
                               (ssl-load-private-key! ctx server-1-certificate #t #f default-locale-encoding)
                               ctx))]
                   [listener (if ssl-server-ctx
                                 (ssl-listen server-1-port (random 123456789) #t server-1-host ssl-server-ctx)
                                 (tcp-listen server-1-port max-allow-wait #t server-1-host))])
              (letrec ([main-loop (λ ()
                                    (send (new ftp-session%
                                               [current-server 1]
                                               [server-params server-params]
                                               [ssl-server-context ssl-server-ctx]
                                               [ssl-client-context ssl-client-ctx]
                                               [disable-commands disable-ftp-commands]
                                               [welcome-message welcome-message])
                                          handle-client-request listener transfer-wait-time)
                                    (main-loop))])
                (set! server-1-thread (thread main-loop)))))
          (when (and server-2-host server-2-port)
            (let* ([ssl-server-ctx
                    (and/exc server-2-encryption server-2-certificate
                             (let ([ctx (ssl-make-server-context server-2-encryption)])
                               (ssl-load-certificate-chain! ctx server-2-certificate default-locale-encoding)
                               (ssl-load-private-key! ctx server-2-certificate #t #f default-locale-encoding)
                               ctx))]
                   [ssl-client-ctx
                    (and/exc server-2-encryption server-2-certificate
                             (let ([ctx (ssl-make-client-context server-2-encryption)])
                               (ssl-load-certificate-chain! ctx server-2-certificate default-locale-encoding)
                               (ssl-load-private-key! ctx server-2-certificate #t #f default-locale-encoding)
                               ctx))]
                   [listener (if ssl-server-ctx
                                 (ssl-listen server-2-port (random 123456789) #t server-2-host ssl-server-ctx)
                                 (tcp-listen server-2-port max-allow-wait #t server-2-host))])
              (letrec ([main-loop (λ ()
                                    (send (new ftp-session%
                                               [current-server 2]
                                               [server-params server-params]
                                               [ssl-server-context ssl-server-ctx]
                                               [ssl-client-context ssl-client-ctx]
                                               [disable-commands disable-ftp-commands]
                                               [welcome-message welcome-message])
                                          handle-client-request listener transfer-wait-time)
                                    (main-loop))])
                (set! server-1-thread (thread main-loop))))))
        (set! state 'running)))
    
    (define/public (stop)
      (when (eq? state 'running)
        (custodian-shutdown-all server-custodian)
        (set! state 'stopped)))
    
    (define/public (status) state)))