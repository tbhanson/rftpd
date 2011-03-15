#|

Racket FTP Server Library v1.4.1
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
         (file "utils.rkt")
         (file "lib-ssl.rkt")
         (file "ftp-vfs.rkt")
         srfi/48)

(provide ftp-server%
         and/exc
         IPv4?
         IPv6?
         host-string?
         port-number?
         ssl-protocol?)

(struct ftp-host&port (host port))
(struct passive-host&ports (host from to))

(struct ftp-server-params
  (passive-1-host&ports
   passive-2-host&ports
   
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
     (SYSTEM (EN . "215 UNIX emulated by RFTPd.")
             (RU . "215 UNIX эмулируется RFTPd."))
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

(provide/contract
 [make-passive-host&ports ((or/c IPv4? IPv6?) port-number? port-number? . -> . passive-host&ports?)])

(define (make-passive-host&ports host from to)
  (and (from . < . to)
       (passive-host&ports host from to)))

(define short-print-log-event
  (let ([sema (make-semaphore 1)])
    (λ (log-out client-host user-id msg [user-name? #t])
      (semaphore-wait sema)
      (if user-name?
          (fprintf log-out
                   "~a [~a] ~a : ~a\n" (date->string (current-date) #t) client-host user-id msg)
          (fprintf log-out
                   "~a [~a] ~a\n" (date->string (current-date) #t) client-host msg))
      (flush-output log-out)
      (semaphore-post sema))))

(define ftp-utils%
  (mixin () ()
    (super-new)
    
    (define/public (get-params str)
      (let ([start 0]
            [end (string-length str)])
        (do [] [(not (memq (string-ref str start) '(#\space #\tab)))]
          (set! start (add1 start)))
        (do [] [(or (>= start end)
                    (memq (string-ref str start) '(#\space #\tab)))]
          (set! start (add1 start)))
        (and (< start end)
             (do [] [(or (>= start end)
                         (not (memq (string-ref str start) '(#\space #\tab))))]
               (set! start (add1 start)))
             (< start end)
             (do [] [(not (memq (string-ref str (sub1 end)) '(#\space #\tab)))]
               (set! end (sub1 end)))
             (substring str start end))))
    
    (define/public (delete-lws str [ws '(#\space #\tab)])
      (do [(i 0 (add1 i))]
        [(not (memq (string-ref str i) ws))
         (substring str i)]))
    
    (define/public (delete-rws str [ws '(#\space #\tab)])
      (do [(i (string-length str) (sub1 i))]
        [(not (memq (string-ref str (sub1 i)) ws))
         (substring str 0 i)]))
    
    (define/public (delete-lrws str [ws '(#\space #\tab)])
      (let ([start 0]
            [end (string-length str)])
        (do [] [(not (memq (string-ref str start) ws))]
          (set! start (add1 start)))
        (do [] [(not (memq (string-ref str (sub1 end)) ws))]
          (set! end (sub1 end)))
        (substring str start end)))
    
    (define/public (get-params* delseq req)
      (let ([p (regexp-match delseq req)])
        (and p (delete-lrws (car p)))))))

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
  (class (ftp-encoding% (ftp-utils% object%))
    
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
    
    (define/public (net-accept tcp-listener)
      (let-values ([(in out) (tcp-accept tcp-listener)])
        (if ssl-server-context 
            (ports->ssl-ports in out 
                              #:mode 'accept
                              #:context ssl-server-context
                              #:close-original? #t
                              #:shutdown-on-close? #t) 
            (values in out))))
    
    (define/public (ftp-data-transfer data [file? #f])
      (case DTP
        ((passive)
         (passive-data-transfer data file?))
        ((active)
         (active-data-transfer data file?))))
    
    (define (active-data-transfer data file?)
      (set! current-process (make-custodian))
      (let ([host (ftp-host&port-host active-host&port)]
            [port (ftp-host&port-port active-host&port)])
        (parameterize ([current-custodian current-process])
          (thread (λ ()
                    (with-handlers ([any/c (λ (e) #|(displayln e)|# (print-abort))])
                      (let-values ([(in out) (tcp-connect host port)])
                        (print-connect)
                        (when ssl-client-context
                          (set!-values (in out) 
                                       (ports->ssl-ports in out 
                                                         #:mode 'connect
                                                         #:context ssl-client-context)))
                        (let-values ([(reset-alarm kill-alarm)
                                      (alarm-clock 1 15
                                                   (λ() (custodian-shutdown-all current-process)))])
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
                    (custodian-shutdown-all current-process))))))
    
    (define (passive-data-transfer data file?)
      (parameterize ([current-custodian current-process])
        (thread (λ ()
                  (with-handlers ([any/c (λ (e) (print-abort))])
                    (let-values ([(in out) (net-accept pasv-listener)])
                      (print-connect)
                      (when ssl-client-context
                        (set!-values (in out) 
                                     (ports->ssl-ports in out 
                                                       #:mode 'connect
                                                       #:context ssl-client-context)))
                      (let-values ([(reset-alarm kill-alarm)
                                    (alarm-clock 1 15
                                                 (λ() (custodian-shutdown-all current-process)))])
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
                  (custodian-shutdown-all current-process)))))
    
    (define/public (ftp-store-file current-ftp-user new-file-full-path exists-mode)
      (case DTP
        ((passive)
         (passive-store-file current-ftp-user new-file-full-path exists-mode))
        ((active)
         (active-store-file current-ftp-user new-file-full-path exists-mode))))
    
    (define (active-store-file current-ftp-user new-file-full-path exists-mode)
      (set! current-process (make-custodian))
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
                          (let-values ([(in out) (tcp-connect host port)])
                            (print-connect)
                            (when restart-marker
                              (file-position fout restart-marker)
                              (set! restart-marker #f))
                            (let-values ([(reset-alarm kill-alarm)
                                          (alarm-clock 1 15
                                                       (λ() (custodian-shutdown-all current-process)))])
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
                                                     (λ() (custodian-shutdown-all current-process)))])
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
                  (custodian-shutdown-all current-process)))))
    
    (define-syntax (server-1-host stx)
      #'(ftp-server-params-server-1-host server-params))
    
    (define-syntax (default-locale-encoding stx)
      #'(ftp-server-params-default-locale-encoding server-params))
    
    (define-syntax (passive-1-ports-from stx)
      #'(passive-host&ports-from (ftp-server-params-passive-1-ports server-params)))
    
    (define-syntax (passive-2-ports-from stx)
      #'(passive-host&ports-from (ftp-server-params-passive-2-ports server-params)))))

(define ftp-session%
  (class ftp-DTP%
    (inherit get-params
             print/encoding
             print-crlf/encoding
             print*-crlf/encoding
             string->bytes/encoding
             read-request
             net-accept
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
                random-gen
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
    (struct ftp-mlst-features (size? modify? perm?) #:mutable)
    
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
    (define *mlst-features* (ftp-mlst-features #t #t #t))
    (define *lang-list* (let ([r (hash-ref server-responses 'SYNTAX-ERROR)])
                          (map car r)))
    (define *current-lang* (car *lang-list*))
    
    (define net-addresses (if ssl-server-context ssl-addresses tcp-addresses))
    (define *cmd-list* null)
    (define *cmd-voc* #f)
    ;;
    ;; ---------- Public Methods ----------
    ;;
    (define/public (handle-client-request tcp-listener transfer-wait-time)
      (let ([cust (make-custodian)])
        (with-handlers ([any/c (λ(e) #|(displayln e)|# (custodian-shutdown-all cust))])
          (parameterize ([current-custodian cust])
            (set!-values (*client-input-port* *client-output-port*) (net-accept tcp-listener))
            (let-values ([(server-host client-host) (net-addresses *client-input-port*)])
              (set! *client-host* client-host)
              (set! *current-protocol* (if (IPv4? server-host) '|1| '|2|))
              (thread (λ ()
                        (accept-client-request (connect-shutdown transfer-wait-time cust))
                        (kill-current-ftp-process)
                        (custodian-shutdown-all cust))))))))
    ;;
    ;; ---------- Private Methods ----------
    ;;
    (define (connect-shutdown time connect-cust)
      (let-values ([(reset kill)
                    (alarm-clock 1 time
                                 (λ()
                                   ;421 No-transfer-time exceeded. Closing control connection.
                                   (custodian-shutdown-all connect-cust)))])
        reset))
    
    (define (accept-client-request [reset-timer void])
      (with-handlers ([any/c #|displayln|# void])
        (print-crlf/encoding** 'WELCOME welcome-message)
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
      (let ([correct
             (cond
               ((string? *user-id*)
                (let ([pass params])
                  (cond
                    ((not (hash-ref ftp-users *user-id* #f))
                     'login-incorrect)
                    ((string=? (ftp-user-pass (hash-ref ftp-users *user-id*))
                               "")
                     'anonymous-logged-in)
                    ((and (hash-ref bad-auth-table *user-id* #f)
                          ((mcar (hash-ref bad-auth-table *user-id*)). >= . max-auth-attempts)
                          (<= ((current-seconds). - .(mcdr (hash-ref bad-auth-table *user-id*)))
                              bad-auth-sleep-sec))
                     (let ([pair (hash-ref bad-auth-table *user-id*)])
                       (set-mcar! pair (add1 (mcar pair)))
                       (set-mcdr! pair (current-seconds)))
                     'login-incorrect)
                    ((not pass)
                     'login-incorrect)
                    ((string=? (ftp-user-pass (hash-ref ftp-users *user-id*))
                               pass)
                     (when (hash-ref bad-auth-table *user-id* #f)
                       (hash-remove! bad-auth-table *user-id*))
                     'user-logged-in)
                    (else
                     (if (hash-ref bad-auth-table *user-id* #f)
                         (let ([pair (hash-ref bad-auth-table *user-id*)])
                           (set-mcar! pair (add1 (mcar pair)))
                           (set-mcdr! pair (current-seconds)))
                         (hash-set! bad-auth-table *user-id* (mcons 1 (current-seconds))))
                     'passw-incorrect))))
               (else
                'login-incorrect))])
        (case correct
          [(user-logged-in)
           (set! *root-dir* (ftp-user-root-dir (hash-ref ftp-users *user-id*)))
           (if (ftp-dir-exists? *root-dir*)
               (begin
                 (set! *user-logged* #t)
                 (print-log-event "User logged in.")
                 (print-crlf/encoding** 'USER-LOGGED *user-id*))
               (begin
                 (set! *user-logged* #f)
                 (print-log-event "Users-config file incorrect.")
                 (print-crlf/encoding** 'LOGIN-INCORRECT)))]
          [(anonymous-logged-in)
           (set! *root-dir* (ftp-user-root-dir (hash-ref ftp-users *user-id*)))
           (if (ftp-dir-exists? *root-dir*)
               (begin
                 (set! *user-logged* #t)
                 (print-log-event "Anonymous user logged in.")
                 (print-crlf/encoding** 'ANONYMOUS-LOGGED))
               (begin
                 (set! *user-logged* #f)
                 (print-log-event "Users-config file incorrect.")
                 (print-crlf/encoding** 'LOGIN-INCORRECT)))]
          [(login-incorrect)
           (set! *user-logged* #f)
           (print-log-event "Login incorrect.")
           (print-crlf/encoding** 'LOGIN-INCORRECT)]
          [(passw-incorrect)
           (set! *user-logged* #f)
           (print-log-event "Password incorrect.")
           (print-crlf/encoding** 'LOGIN-INCORRECT)])))
    
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
            (and (hash-ref *cmd-voc* "CLNT" #f)
                 (print-crlf/encoding* " CLNT"))
            (and (hash-ref *cmd-voc* "LANG" #f)
                 (print-crlf/encoding* (string-append
                                        " LANG "
                                        (string-join (map (λ(l)
                                                            (string-append (symbol->string l)
                                                                           (if (eq? l *current-lang*) "*" "")))
                                                          *lang-list*)
                                                     ";"))))
            (and (hash-ref *cmd-voc* "EPRT" #f)
                 (print-crlf/encoding* " EPRT"))
            (and (hash-ref *cmd-voc* "EPSV" #f)
                 (print-crlf/encoding* " EPSV"))
            (print-crlf/encoding* " UTF8")
            (and (hash-ref *cmd-voc* "REST" #f)
                 (print-crlf/encoding* " REST STREAM"))
            (and (hash-ref *cmd-voc* "MLST" #f)
                 (print-crlf/encoding* (format " MLST size~a;modify~a;perm~a"
                                               (if (ftp-mlst-features-size? *mlst-features*) "*" "")
                                               (if (ftp-mlst-features-modify? *mlst-features*) "*" "")
                                               (if (ftp-mlst-features-perm? *mlst-features*) "*" ""))))
            (and (hash-ref *cmd-voc* "MLSD" #f)
                 (print-crlf/encoding* " MLSD"))
            (and (hash-ref *cmd-voc* "SIZE" #f)
                 (print-crlf/encoding* " SIZE"))
            (and (hash-ref *cmd-voc* "MDTM" #f)
                 (print-crlf/encoding* " MDTM"))
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
      (if (and params
               (regexp-match #rx"^[0-9]+,[0-9]+,[0-9]+,[0-9]+,[0-9]+,[0-9]+$" params))
          (let* ([l (string-split-char #\, params)]
                 [host (string-append (first l) "." (second l) "." (third l) "." (fourth l))]
                 [port (((string->number (fifth l)). * . 256). + .(string->number (sixth l)))])
            (if (and (IPv4? host) (port-number? port))
                (begin
                  (set! DTP 'active)
                  ;(set! active-host&port (ftp-host&port host port))
                  (set! active-host&port (ftp-host&port (if (private-IPv4? host) *client-host* host) port))
                  (print-crlf/encoding** 'CMD-SUCCESSFUL 200 "PORT"))
                (print-crlf/encoding** 'SYNTAX-ERROR "")))
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
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
           (string-append (month->string (date-month dte)) " " (number->string (date-day dte)) " "
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
                                      [ftp-full-spath (string-append ftp-dir-name "/" spath)]
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
          (if (not dir)
              (dlst *current-dir*)
              (let ([spath (build-ftp-spath* dir)])
                (if (and (ftp-dir-exists? (string-append *root-dir* spath))
                         (ftp-perm-allow? spath))
                    (if (ftp-dir-allow-execute*? (string-append *root-dir* spath))
                        (dlst spath)
                        (unless status
                          (print-crlf/encoding** 'PERM-DENIED)))
                    (unless status
                      (print-crlf/encoding** 'DIR-NOT-FOUND))))))))
    
    (define (RETR-COMMAND params)
      (local [(define (fcopy full-path-file)
                (ftp-data-transfer full-path-file #t))]
        
        (if (not params)
            (print-crlf/encoding** 'SYNTAX-ERROR "")
            (let ([spath (build-ftp-spath* params)])
              (if (and (ftp-file-exists? (string-append *root-dir* spath))
                       (ftp-perm-allow? spath))
                  (if (ftp-file-allow-read*? (string-append *root-dir* spath))
                      (fcopy (string-append *root-dir* spath))
                      (print-crlf/encoding** 'PERM-DENIED))
                  (print-crlf/encoding** 'FILE-NOT-FOUND))))))
    
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
      (if (not params)
          (print-crlf/encoding** 'SYNTAX-ERROR "")
          (let ([spath (build-ftp-spath* params)])
            (if (and (ftp-dir-exists? (string-append *root-dir* spath))
                     (ftp-perm-allow? spath))
                (begin
                  (set! *current-dir* (simplify-ftp-path spath))
                  (print-crlf/encoding** 'CMD-SUCCESSFUL 250 "CWD"))
                (print-crlf/encoding** 'DIR-NOT-FOUND)))))
    
    (define (MKD-COMMAND params)
      (local [(define (mkd ftp-parent-path dir-name user)
                (if (ftp-vfs-obj-access-allow*? ftp-parent-path)
                    (let* ([full-parent-path (string-append *root-dir* ftp-parent-path)]
                           [sp (string-append full-parent-path "/" dir-name)])
                      (if (ftp-dir-allow-write*? full-parent-path)
                          (if (ftp-dir-exists? sp)
                              (print-crlf/encoding** 'DIR-EXIST)
                              (let* ([fpp (simplify-ftp-path ftp-parent-path)]
                                     [fp (string-append fpp (if (string=? fpp "/") "" "/") dir-name)])
                                (ftp-mkdir sp (ftp-user-login user) (ftp-user-group user))
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
                 (if (ftp-vfs-obj-access-allow*? *current-dir*)
                     (print-crlf/encoding** 'DIR-EXIST)
                     (print-crlf/encoding** 'CREATE-DIR-PERM-DENIED)))
                ((not parent-path)
                 (mkd *current-dir* dir-name user))
                (else
                 (let ([spath (build-ftp-spath* parent-path)])
                   (if (ftp-dir-exists? (string-append *root-dir* spath))
                       (mkd spath dir-name user)
                       (print-crlf/encoding** 'CANT-CREATE-DIR))))))
            (print-crlf/encoding** 'SYNTAX-ERROR ""))))
    
    (define (RMD-COMMAND params)
      (local
        [(define (rmd ftp-path)
           (if (ftp-perm-allow? ftp-path)
               (let ([spath (string-append *root-dir* ftp-path)])
                 (if ((file-or-directory-identity spath). = .(file-or-directory-identity *root-dir*))
                     (print-crlf/encoding** 'DIR-NOT-FOUND)
                     (if (ftp-dir-allow-write*? (string-append *root-dir* (simplify-ftp-path ftp-path 1)))
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
        
        (let ([spath (build-ftp-spath* params)])
          (if (ftp-dir-exists? (string-append *root-dir* spath))
              (rmd spath)
              (print-crlf/encoding** 'DIR-NOT-FOUND)))))
    
    (define (STORE-FILE params [exists-mode 'truncate])
      (local [(define (stor ftp-parent-path file-name)
                (if (ftp-vfs-obj-access-allow*? ftp-parent-path)
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
                (else
                 (let ([spath (build-ftp-spath* parent-path)])
                   (if (ftp-dir-exists? (string-append *root-dir* spath))
                       (stor spath file-name)
                       (print-crlf/encoding** 'CANT-STORE-FILE))))))
            (print-crlf/encoding** 'SYNTAX-ERROR ""))))
    
    (define (STOU-FILE params)
      (cond
        (params
         (print-crlf/encoding** 'SYNTAX-ERROR ""))
        ((and (ftp-vfs-obj-access-allow*? *current-dir*)
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
                 (if (ftp-dir-allow-write*? (string-append *root-dir* (simplify-ftp-path ftp-path 1)))
                     (with-handlers ([exn:fail:filesystem? (λ (e)
                                                             (print-crlf/encoding* "550 System error."))])
                       (delete-file (string-append spath ftp-vfs-file-spath))
                       (delete-file spath)
                       (print-log-event (format "Delete a file ~a" (simplify-ftp-path ftp-path)))
                       (print-crlf/encoding** 'CMD-SUCCESSFUL 250 "DELE"))
                     (print-crlf/encoding** 'DELFILE-PERM-DENIED)))
               (print-crlf/encoding** 'FILE-NOT-FOUND)))]
        
        (let ([spath (build-ftp-spath* params)])
          (if (ftp-file-exists? (string-append *root-dir* spath))
              (dele spath)
              (print-crlf/encoding** 'FILE-NOT-FOUND)))))
    
    (define (SITE-COMMAND params)
      (local
        [(define (chmod permis path)
           (let* ([spath (build-ftp-spath* path)]
                  [permis (if ((length permis). = . 4)
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
                                [info (ftp-file-or-dir-full-info (string-append full-path target))]
                                [owner (vector-ref info 1)]
                                [group (vector-ref info 2)])
                           (if (or (string=? owner (ftp-user-login current-ftp-user))
                                   (member-ftp-group? ftp-groups current-ftp-user "root"))
                               (begin
                                 (ftp-mksys-file (string-append full-path target)
                                                 owner group (get-permis permis))
                                 (print-log-event (format "Change the permissions of a ~a" 
                                                          (simplify-ftp-path spath)))
                                 (print-crlf/encoding** 'CMD-SUCCESSFUL 200 "SITE CHMOD"))
                               (print-crlf/encoding** 'PERM-DENIED)))
                         (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)))])
             (cond
               ((ftp-file-exists? (string-append *root-dir* spath))
                (fchmod spath ftp-vfs-file-spath))
               ((ftp-dir-exists? (string-append *root-dir* spath))
                (fchmod spath ftp-vfs-dir-spath))
               (else
                (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)))))
         
         (define (chown owner path)
           (let* ([spath (build-ftp-spath* path)]
                  [fchown 
                   (λ (ftp-spath target)
                     (if (ftp-perm-allow? ftp-spath)
                         (let* ([full-path (string-append *root-dir* ftp-spath)]
                                [info (ftp-file-or-dir-full-info (string-append full-path target))]
                                [perm (vector-ref info 0)]
                                [group (vector-ref info 2)])
                           (if (and (hash-ref ftp-users owner #f)
                                    (or (member-ftp-group? ftp-groups current-ftp-user "root")
                                        (string=? owner (ftp-user-login current-ftp-user))))
                               (begin
                                 (ftp-mksys-file (string-append full-path target)
                                                 owner group perm)
                                 (print-log-event (format "Change the owner of a ~a" 
                                                          (simplify-ftp-path spath)))
                                 (print-crlf/encoding** 'CMD-SUCCESSFUL 200 "SITE CHOWN"))
                               (print-crlf/encoding** 'PERM-DENIED)))
                         (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)))])
             (cond
               ((ftp-file-exists? (string-append *root-dir* spath))
                (fchown spath ftp-vfs-file-spath))
               ((ftp-dir-exists? (string-append *root-dir* spath))
                (fchown spath ftp-vfs-dir-spath))
               (else
                (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)))))
         
         (define (chgrp group path)
           (let* ([spath (build-ftp-spath* path)]
                  [fchgrp
                   (λ (ftp-spath target)
                     (if (ftp-perm-allow? ftp-spath)
                         (let* ([full-path (string-append *root-dir* ftp-spath)]
                                [info (ftp-file-or-dir-full-info (string-append full-path target))]
                                [perm (vector-ref info 0)]
                                [owner (vector-ref info 1)])
                           (if (and (hash-ref ftp-groups group #f)
                                    (or (member-ftp-group? ftp-groups current-ftp-user group)
                                        (member-ftp-group? ftp-groups current-ftp-user "root")))
                               (begin
                                 (ftp-mksys-file (string-append full-path target)
                                                 owner group perm)
                                 (print-log-event (format "Change the group of a ~a" 
                                                          (simplify-ftp-path spath)))
                                 (print-crlf/encoding** 'CMD-SUCCESSFUL 200 "SITE CHGRP"))
                               (print-crlf/encoding** 'PERM-DENIED)))
                         (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)))])
             (cond
               ((ftp-file-exists? (string-append *root-dir* spath))
                (fchgrp spath ftp-vfs-file-spath))
               ((ftp-dir-exists? (string-append *root-dir* spath))
                (fchgrp spath ftp-vfs-dir-spath))
               (else
                (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)))))]
        
        (if params
            (let ([cmd (string->symbol (string-upcase (car (regexp-match #rx"[^ ]+" params))))])
              (case cmd
                [(CHMOD)
                 (let* ([permis+path (get-params params)]
                        [permis (regexp-match #rx"[0-7]?[0-7][0-7][0-7]" permis+path)])
                   (if permis
                       (let ([path (get-params permis+path)])
                         (if path
                             (chmod (string->list (car permis)) path)
                             (print-crlf/encoding** 'SYNTAX-ERROR "CHMOD:")))
                       (print-crlf/encoding** 'SYNTAX-ERROR "CHMOD:")))]
                [(CHOWN)
                 (let* ([owner+path (get-params params)]
                        [owner (regexp-match #rx"[^ ]+" owner+path)])
                   (if owner
                       (let ([path (get-params owner+path)])
                         (if path
                             (chown (car owner) path)
                             (print-crlf/encoding** 'SYNTAX-ERROR "CHOWN:")))
                       (print-crlf/encoding** 'SYNTAX-ERROR "CHOWN:")))]
                [(CHGRP)
                 (let* ([group+path (get-params params)]
                        [group (regexp-match #rx"[^ ]+" group+path)])
                   (if group
                       (let ([path (get-params group+path)])
                         (if path
                             (chgrp (car group) path)
                             (print-crlf/encoding** 'SYNTAX-ERROR "CHGRP:")))
                       (print-crlf/encoding** 'SYNTAX-ERROR "CHGRP:")))]
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
            (mdtm (build-ftp-spath* params)))
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
            (size (build-ftp-spath* params)))
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
        
        (if params
            (let* ([spath (build-ftp-spath* params)]
                   [path (string-append *root-dir* spath)])
              (cond
                ((ftp-file-exists? path)
                 (mlst spath))
                ((ftp-dir-exists? path)
                 (if ((file-or-directory-identity path). = .(file-or-directory-identity *root-dir*))
                     (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)
                     (mlst spath)))
                (else
                 (print-crlf/encoding** 'FILE-DIR-NOT-FOUND))))
            (let ([path (string-append *root-dir* *current-dir*)])
              (if ((file-or-directory-identity path). = .(file-or-directory-identity *root-dir*))
                  (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)
                  (mlst *current-dir*))))))
    
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
        
        (if params
            (let* ([spath (build-ftp-spath* params)]
                   [path (string-append *root-dir* spath)])
              (if (ftp-dir-exists? path)
                  (mlsd spath)
                  (print-crlf/encoding** 'DIR-NOT-FOUND)))
            (mlsd *current-dir*))))
    
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
                           (begin
                             (set! *mlst-features* (ftp-mlst-features #f #f #f))
                             (for-each (λ (mode)
                                         (case mode
                                           ((SIZE) (set-ftp-mlst-features-size?! *mlst-features* #t))
                                           ((MODIFY) (set-ftp-mlst-features-modify?! *mlst-features* #t))
                                           ((PERM) (set-ftp-mlst-features-perm?! *mlst-features* #t))))
                                       mlst)
                             (print-crlf/encoding** 'MLST-ON))
                           (print-crlf/encoding** 'SYNTAX-ERROR "MLST:")))
                     (print-crlf/encoding** 'SYNTAX-ERROR "MLST:"))))
              (else (print-crlf/encoding** 'SYNTAX-ERROR ""))))
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    (define (RNFR-COMMAND params)
      (if params
          (let* ([spath (build-ftp-spath* params)]
                 [path (string-append *root-dir* spath)])
            (cond
              ((ftp-file-exists? path)
               (if (ftp-perm-allow? spath)
                   (if (ftp-dir-allow-write*? (string-append *root-dir* (simplify-ftp-path spath 1)))
                       (begin
                         (set! *rename-path* path)
                         (print-crlf/encoding** 'RENAME-OK))
                       (print-crlf/encoding** 'PERM-DENIED))
                   (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)))
              ((ftp-dir-exists? path)
               (if (ftp-perm-allow? spath)
                   (if (ftp-dir-allow-write*? (string-append *root-dir* (simplify-ftp-path spath 1)))
                       (begin
                         (set! *rename-path* path)
                         (print-crlf/encoding** 'RENAME-OK))
                       (print-crlf/encoding** 'PERM-DENIED))
                   (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)))
              (else
               (print-crlf/encoding** 'FILE-DIR-NOT-FOUND))))
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    (define (RNTO-COMMAND params)
      (local [(define (move file? old-path ftp-parent-path name user)
                (if (ftp-vfs-obj-access-allow*? ftp-parent-path)
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
                    (else
                     (let ([spath (build-ftp-spath* parent-path)])
                       (if (ftp-dir-exists? (string-append *root-dir* spath))
                           (move old-file? old-path spath name user)
                           (print-crlf/encoding** 'CANT-RENAME)))))
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
      #'(passive-host&ports-host (ftp-server-params-passive-1-host&ports server-params)))
    
    (define-syntax (passive-2-host stx)
      #'(passive-host&ports-host (ftp-server-params-passive-2-host&ports server-params)))
    
    (define-syntax (passive-1-ports-from stx)
      #'(passive-host&ports-from (ftp-server-params-passive-1-host&ports server-params)))
    
    (define-syntax (passive-1-ports-to stx)
      #'(passive-host&ports-to (ftp-server-params-passive-1-host&ports server-params)))
    
    (define-syntax (passive-2-ports-from stx)
      #'(passive-host&ports-from (ftp-server-params-passive-2-host&ports server-params)))
    
    (define-syntax (passive-2-ports-to stx)
      #'(passive-host&ports-to (ftp-server-params-passive-2-host&ports server-params)))
    
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
    
    (define-syntax (ftp-vfs-obj-access-allow*? stx)
      (syntax-case stx ()
        [(_ ftp-full-spath)
         #'(ftp-vfs-obj-access-allow? *root-dir* ftp-full-spath current-ftp-user ftp-groups)]
        [(_ ftp-full-spath drop)
         #'(ftp-vfs-obj-access-allow? *root-dir* ftp-full-spath current-ftp-user ftp-groups drop)]))
    
    (define-syntax-rule (ftp-perm-allow? ftp-full-spath)
      (ftp-vfs-obj-access-allow*? ftp-full-spath 1))
    
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
    
    (define-syntax-rule (build-ftp-spath* ftp-spath ...)
      (build-ftp-spath *current-dir* ftp-spath ...))
    
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
             [parent-path (and (not (string=? "/" ftp-path))
                               (string-append *root-dir* (simplify-ftp-path ftp-path 1)))]
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
                                   (if (string=? group "root")
                                       (if file? "rawfd" "elcmfd")
                                       (let ([i (cond
                                                  ((string=? (vector-ref info 1) user-name) 8)
                                                  ((string=? (vector-ref info 2) group) 5)
                                                  (else 2))])
                                         (string-append 
                                          (if (and (bitwise-bit-set? sysbytes (- i 2)) (not file?)) "e" "")
                                          (if (bitwise-bit-set? sysbytes i) (if file? "r" "l") "")
                                          (if (bitwise-bit-set? sysbytes (sub1 i)) (if file? "a" "cm") "")
                                          (if (and parent-path
                                                   (ftp-dir-allow-write*? parent-path))
                                              (if file? "wfd" "fd") "")))))
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
                       
                       [passive-1-host&ports    passive-host&ports?]
                       [passive-2-host&ports    passive-host&ports?]
                       
                       [default-root-dir        not-null-string/c]
                       [default-locale-encoding string?]
                       [log-output-port         output-port?])
           [add-ftp-user (string?
                          not-null-string/c string?
                          not-null-string/c (non-empty-listof not-null-string/c) not-null-string/c . ->m . void?)]
           [add-ftp-group (not-null-string/c (listof not-null-string/c) . ->m . void?)])
  
  (class (ftp-utils% object%)
    (super-new)
    (inherit delete-lrws)
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
                
                [passive-1-host&ports    (make-passive-host&ports "127.0.0.1" 40000 40999)]
                [passive-2-host&ports    (make-passive-host&ports "127.0.0.1" 40000 40999)]
                
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
    (define random-1-gen (make-pseudo-random-generator))
    (define random-2-gen (make-pseudo-random-generator))
    ;;
    ;; ---------- Public Methods ----------
    ;;
    (define/public (add-ftp-user full-name login pass group home-dirs [root-dir default-root-dir])
      (let ([root-dir (delete-lrws root-dir)])
        (hash-set! server-ftp-users login (ftp-user full-name login pass group home-dirs root-dir))
        (unless (ftp-dir-exists? root-dir)
          (ftp-mkdir root-dir login group))
        (for-each (λ (home-dir)
                    (unless (ftp-dir-exists? (string-append root-dir home-dir))
                      (let ([dirs (filter (λ (s) (not (string=? s "")))
                                          (regexp-split #rx"[/\\\\]+" home-dir))]
                            [curr-dir ""])
                        (unless (zero? (length dirs))
                          (for-each (λ (p)
                                      (unless (ftp-dir-exists? (string-append root-dir curr-dir "/" p))
                                        (ftp-mkdir (string-append root-dir curr-dir "/" p)
                                                   login group))
                                      (set! curr-dir (string-append curr-dir "/" p)))
                                    (drop-right dirs 1)))
                        (ftp-mkdir (string-append root-dir home-dir) login group))))
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
        (set! server-params (ftp-server-params passive-1-host&ports
                                               passive-2-host&ports
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
          (ftp-mkdir default-root-dir))
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
                   [tcp-listener (tcp-listen server-1-port max-allow-wait #t server-1-host)])
              (letrec ([main-loop (λ ()
                                    (send (new ftp-session%
                                               [current-server 1]
                                               [server-params server-params]
                                               [ssl-server-context ssl-server-ctx]
                                               [ssl-client-context ssl-client-ctx]
                                               [disable-commands disable-ftp-commands]
                                               [random-gen random-1-gen]
                                               [welcome-message welcome-message])
                                          handle-client-request tcp-listener transfer-wait-time)
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
                   [tcp-listener (tcp-listen server-2-port max-allow-wait #t server-2-host)])
              (letrec ([main-loop (λ ()
                                    (send (new ftp-session%
                                               [current-server 2]
                                               [server-params server-params]
                                               [ssl-server-context ssl-server-ctx]
                                               [ssl-client-context ssl-client-ctx]
                                               [disable-commands disable-ftp-commands]
                                               [random-gen random-2-gen]
                                               [welcome-message welcome-message])
                                          handle-client-request tcp-listener transfer-wait-time)
                                    (main-loop))])
                (set! server-2-thread (thread main-loop))))))
        (set! state 'running)))
    
    (define/public (stop)
      (when (eq? state 'running)
        (custodian-shutdown-all server-custodian)
        (set! state 'stopped)))
    
    (define/public (status) state)))