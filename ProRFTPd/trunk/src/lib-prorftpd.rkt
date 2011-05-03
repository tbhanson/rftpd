#|

ProRFTPd Library v1.0.7
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

(require racket/date
         (file "debug.rkt")
         (file "utils.rkt")
         (file "lib-string.rkt")
         (file "lib-ssl.rkt")
         (file "platform.rkt")
         (file "system.rkt")
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

(date-display-format 'iso-8601)

;;
;; ---------- Global Definitions ----------
;;

(define default-server-responses
  (make-hash
   '((SYNTAX-ERROR (EN . "501 ~a Syntax error in parameters or arguments.")
                   (RU . "501 ~a Синтаксическая ошибка (неверный параметр или аргумент)."))
     (MAX-CLIENTS-PER-IP (EN . "530 You have exceeded the limit on connections, please try again later.")
                         (RU . "530 Вы превысили лимит подключений, пожалуйста, повторите попытку позже."))
     (TRANSFER-WAIT-TIME (EN . "421 Closing control connection due to inactivity from the client.")
                         (RU . "421 Управляющее соединение закрывается из-за отсутствия активности со стороны клиента."))
     (CMD-NOT-IMPLEMENTED (EN . "502 ~a not implemented.")
                          (RU . "502 Команда ~a не реализована."))
     (INVALID-CMD-SYNTAX (EN . "500 Invalid command syntax.")
                         (RU . "500 Неверный синтаксис команды."))
     (PLEASE-LOGIN (EN . "530 Please login with USER and PASS.")
                   (RU . "530 Пожалуйста, авторизируйтесь используя USER и PASS."))
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
     (SYSTEM (EN . "215 UNIX Type: L8")
             (RU . "215 UNIX Type: L8"))
     (CURRENT-DIR (EN . "257 ~s is current directory.")
                  (RU . "257 ~s - текущий каталог."))
     (CMD-SUCCESSFUL (EN . "~a ~a command successful.")
                     (RU . "~a Команда ~a выполнена."))
     (CMD-FAILED (EN . "~a ~a command failed.")
                 (RU . "~a Команда ~a не выполнена."))
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
     (DIR-EXIST (EN . "550 Can't create directory. File or directory exist!")
                (RU . "550 Невозможно создать каталог. Каталог или файл уже существует!"))
     (DIR-CREATED (EN . "257 ~s - directory successfully created.")
                  (RU . "257 ~s - создан каталог."))
     (CREATE-DIR-PERM-DENIED (EN . "550 Can't create directory. Permission denied!")
                             (RU . "550 Невозможно создать каталог. Доступ запрещен!"))
     (CANT-CREATE-DIR (EN . "550 Can't create directory.")
                      (RU . "550 Невозможно создать каталог."))
     (CANT-RMDIR (EN . "550 Can't remove directory.")
                 (RU . "550 Невозможно удалить каталог."))
     (DELDIR-NOT-EMPTY (EN . "550 Can't delete directory. Directory not empty!")
                       (RU . "550 Не удается удалить каталог. Каталог не пуст!"))
     (DELDIR-PERM-DENIED (EN . "550 Can't delete directory. Permission denied!")
                         (RU ."550 Не удается удалить каталог. Доступ запрещен!"))
     (APPEND-FILE-PERM-DENIED (EN . "550 Can't append file. Permission denied!")
                              (RU . "550 Невозможно дописать в файл. Доступ запрещен!"))
     (STORE-FILE-PERM-DENIED (EN . "550 Can't store file. Permission denied!")
                             (RU . "550 Невозможно сохранить файл. Доступ запрещен!"))
     (DELFILE-PERM-DENIED (EN . "550 Can't delete file. Permission denied!")
                          (RU . "550 Не удается удалить файл. Доступ запрещен!"))
     (FILE-NOT-FOUND (EN . "550 File not found.")
                     (RU . "550 Файл не найден."))
     (FILE-DIR-NOT-FOUND (EN . "550 No such file or directory.")
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
     (CANT-RENAME-EXIST (EN . "550 Can't rename file or directory. File or directory exist!")
                        (RU . "550 Невозможно переименовать файл или каталог. Каталог или файл уже существует!"))
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
                   (RU . "522 Неверный сетевой протокол."))
     (FORBIDDEN-ADDR-PORT (EN . "505 Forbidden address or port.")
                          (RU . "505 Запрещенный адрес или порт."))
     (MFMT-OK (EN . "213 Modify=~a; ~a")
              (RU . "213 Modify=~a; ~a"))
     (CANT-MFMT (EN . "550 Can't modify a last modification time.")
                (RU . "550 Невозможно изменить время последней модификации."))
     (MFF-OK (EN . "213 ~a ~a")
             (RU . "213 ~a ~a"))
     (UNKNOWN-ERROR (EN . "410 Unknown error.")
                    (RU . "410 Неизвестная ошибка.")))))

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
    
    (define/public (get-params* delseq req)
      (let ([p (regexp-match delseq req)])
        (and p (delete-lrws (car p)))))))

(define ftp-encoding%
  (mixin () ()
    (super-new)
    
    (init-field [default-locale-encoding "UTF-8"])
    
    (define/public (bytes->bytes/encoding encoding bstr)
      (let*-values ([(conv) (bytes-open-converter encoding "UTF-8")]
                    [(result len status) (bytes-convert conv bstr)])
        (bytes-close-converter conv)
        (unless (eq? status 'complete)
          (set! conv (bytes-open-converter default-locale-encoding "UTF-8"))
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
      (let ([line (read-bytes-line input-port)])
        (if (eof-object? line)
            line
            (let ([s (bytes->string/encoding encoding line)])
              (substring s 0 (sub1 (string-length s)))))))
    
    (define/public (print-crlf/encoding encoding str out)
      (print/encoding encoding str out)
      (write-bytes #"\r\n" out)
      (flush-output out))
    
    (define/public (printf-crlf/encoding encoding out form . args)
      (print/encoding encoding (apply format form args) out)
      (write-bytes #"\r\n" out)
      (flush-output out))
    
    (define/public (print*-crlf/encoding encoding out current-lang server-responses response-tag . args)
      (let ([response (cdr (assq current-lang (hash-ref server-responses response-tag)))])
        (print-crlf/encoding encoding (if (null? args) response (apply format response args)) out)))))

(define ftp-DTP%
  (mixin () ()
    (super-new)
    
    (init-field server-host
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
                [log-store-event (λ(new-file-full-path exists-mode) #f)]
                [log-copy-event (λ(file-full-path) #f)])
    
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
                    (with-handlers ([any/c (λ (e) 
                                             ;(displayln e)
                                             (print-abort))])
                      (let-values ([(in out) (tcp-connect host port)])
                        (print-connect)
                        (when ssl-client-context
                          (set!-values (in out) 
                                       (ports->ssl-ports in out 
                                                         #:mode 'connect
                                                         #:context ssl-client-context)))
                        (let-values ([(start-timer pause-timer reset-timer kill-timer)
                                      (alarm-clock 1 15
                                                   (λ() (custodian-shutdown-all current-process)))])
                          (start-timer)
                          (if file?
                              (call-with-input-file data
                                (λ (in)
                                  (let loop ([dat (read-bytes 1048576 in)])
                                    (reset-timer)
                                    (unless (eof-object? dat)
                                      (write-bytes dat out)
                                      (loop (read-bytes 1048576 in))))))
                              (case representation-type
                                ((ASCII)
                                 (print-ascii data out))
                                ((Image)
                                 (write-bytes data out))))
                          (kill-timer))
                        (flush-output out)
                        (when file? (log-copy-event data))
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
                      (let-values ([(start-timer pause-timer reset-timer kill-timer)
                                    (alarm-clock 1 15
                                                 (λ() (custodian-shutdown-all current-process)))])
                        (start-timer)
                        (if file?
                            (call-with-input-file data
                              (λ (in)
                                (let loop ([dat (read-bytes 1048576 in)])
                                  (reset-timer)
                                  (unless (eof-object? dat)
                                    (write-bytes dat out)
                                    (loop (read-bytes 1048576 in))))))
                            (case representation-type
                              ((ASCII)
                               (print-ascii data out))
                              ((Image)
                               (write-bytes data out))))
                        (kill-timer))
                      ;(flush-output out)
                      (close-output-port out);ssl required
                      (when file? (log-copy-event data))
                      (print-close)))
                  (custodian-shutdown-all current-process)))))
    
    (define/public (ftp-store-file userstruct new-file-full-path exists-mode)
      (case DTP
        ((passive)
         (passive-store-file userstruct new-file-full-path exists-mode))
        ((active)
         (active-store-file userstruct new-file-full-path exists-mode))))
    
    (define (active-store-file userstruct new-file-full-path exists-mode)
      (set! current-process (make-custodian))
      (let ([host (ftp-host&port-host active-host&port)]
            [port (ftp-host&port-port active-host&port)])
        (parameterize ([current-custodian current-process])
          (thread (λ ()
                    (with-handlers ([any/c (λ (e) (print-abort))])
                      (let ([exists? (file-exists? new-file-full-path)])
                        (call-with-output-file new-file-full-path
                          (λ (fout)
                            (unless exists? 
                              (ftp-chown new-file-full-path
                                         (ftp-user-uid userstruct) (ftp-user-gid userstruct)))
                            (let-values ([(in out) (tcp-connect host port)])
                              (print-connect)
                              (when ssl-client-context
                                (set!-values (in out) 
                                             (ports->ssl-ports in out 
                                                               #:mode 'connect
                                                               #:context ssl-client-context)))
                              (when restart-marker
                                (file-position fout restart-marker)
                                (set! restart-marker #f))
                              (let-values ([(start-timer pause-timer reset-timer kill-timer)
                                            (alarm-clock 1 15
                                                         (λ() (custodian-shutdown-all current-process)))])
                                (start-timer)
                                (let loop ([dat (read-bytes 1048576 in)])
                                  (reset-timer)
                                  (unless (eof-object? dat)
                                    (write-bytes dat fout)
                                    (loop (read-bytes 1048576 in))))
                                (kill-timer))
                              (flush-output fout)
                              (log-store-event new-file-full-path exists-mode)
                              (print-close)))
                          #:mode 'binary
                          #:exists exists-mode)))
                    (custodian-shutdown-all current-process))))))
    
    (define (passive-store-file userstruct new-file-full-path exists-mode)
      (parameterize ([current-custodian current-process])
        (thread (λ ()
                  (with-handlers ([any/c (λ (e) (print-abort))])
                    (let ([exists? (file-exists? new-file-full-path)])
                      (call-with-output-file new-file-full-path
                        (λ (fout)
                          (unless exists? 
                            (ftp-chown new-file-full-path
                                       (ftp-user-uid userstruct) (ftp-user-gid userstruct)))
                          (let-values ([(in out) (net-accept pasv-listener)])
                            (print-connect)
                            (when restart-marker
                              (file-position fout restart-marker)
                              (set! restart-marker #f))
                            (let-values ([(start-timer pause-timer reset-timer kill-timer)
                                          (alarm-clock 1 15
                                                       (λ() (custodian-shutdown-all current-process)))])
                              (start-timer)
                              (let loop ([dat (read-bytes 1048576 in)])
                                (reset-timer)
                                (unless (eof-object? dat)
                                  (write-bytes dat fout)
                                  (loop (read-bytes 1048576 in))))
                              (kill-timer))
                            (flush-output fout)
                            (log-store-event new-file-full-path exists-mode)
                            (print-close)))
                        #:mode 'binary
                        #:exists exists-mode)))
                  (custodian-shutdown-all current-process)))))))

(define ftp-session%
  (class (ftp-DTP% (ftp-encoding% (ftp-utils% object%)))
    (inherit get-params
             print/encoding
             print-crlf/encoding
             printf-crlf/encoding
             print*-crlf/encoding
             string->bytes/encoding
             read-request
             net-accept
             ftp-data-transfer
             ftp-store-file)
    
    (inherit-field server-host
                   ssl-server-context
                   ssl-client-context
                   default-locale-encoding
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
                pasv-host&ports
                users-table
                clients-table
                random-gen
                server-responses
                default-root-dir
                bad-auth-sleep-sec
                max-auth-attempts
                bad-auth-table
                pass-sleep-sec
                allow-foreign-address
                log-output-port
                [disable-commands null])
    ;;
    ;; ---------- Superclass Initialization ----------
    ;;
    (super-new [print-abort (λ() (print-crlf/encoding** 'TRANSFER-ABORTED))]
               [print-connect (λ() (print-crlf/encoding** 'OPEN-DATA-CONNECTION representation-type))]
               [print-close (λ() (print-crlf/encoding** 'TRANSFER-OK))]
               [print-ascii (λ(data out) (print/encoding *locale-encoding* data out))]
               [log-store-event (λ(new-file-full-path exists-mode)
                                  (print-log-event
                                   (format "~a data to file ~a"
                                           (if (eq? exists-mode 'append) "Append" "Store")
                                           (real-path->ftp-path new-file-full-path *root-dir*))))]
               [log-copy-event (λ(file-full-path)
                                 (print-log-event
                                  (string-append "Read file "
                                                 (real-path->ftp-path file-full-path *root-dir*))))])
    ;;
    ;; ---------- Private Definitions ----------
    ;;
    (struct ftp-mlst-features (type? size? modify? perm? unique? charset?
                                     unix-mode? unix-owner? unix-group?) #:mutable)
    
    (define *client-host* #f)
    (define *client-input-port* #f)
    (define *client-output-port* #f)
    (define *current-protocol* #f)
    (define *locale-encoding* default-locale-encoding)
    (define *login* #f)
    (define *userstruct* #f)
    (define *root-dir* default-root-dir)
    (define *current-dir* "/")
    (define *rename-path* #f)
    (define *mlst-features* (ftp-mlst-features #t #t #t #t #t #t #t #t #t))
    (define *lang-list* (let ([r (hash-ref server-responses 'SYNTAX-ERROR)])
                          (map car r)))
    (define *current-lang* (car *lang-list*))
    
    (define net-addresses (if ssl-server-context ssl-addresses tcp-addresses))
    (define *cmd-list* null)
    (define *cmd-voc* #f)
    (define *quit* #f)
    ;;
    ;; ---------- Public Methods ----------
    ;;
    (define/public (handle-client-request tcp-listener transfer-wait-time max-clients-per-ip)
      (let ([cust (make-custodian)])
        (with-handlers ([any/c (λ (e)
                                 ;(displayln e) 
                                 (custodian-shutdown-all cust))])
          (parameterize ([current-custodian cust])
            (let next-accept ()
              (set!-values (*client-input-port* *client-output-port*) (net-accept tcp-listener))
              (let-values ([(server-host client-host) (net-addresses *client-input-port*)]
                           [(allow?) #t])
                (if (hash-ref clients-table client-host #f)
                    (if (< (hash-ref clients-table client-host) max-clients-per-ip)
                        (hash-set! clients-table client-host (add1 (hash-ref clients-table client-host)))
                        (set! allow? #f))
                    (hash-set! clients-table client-host 1))
                (if allow?
                    (begin
                      (set! *client-host* client-host)
                      (set! *current-protocol* (if (IPv4? server-host) '|1| '|2|))
                      (thread (λ ()
                                (call/cc
                                 (λ (quit)
                                   (set! *quit* quit)
                                   (accept-client-request transfer-wait-time)))
                                (when (hash-ref clients-table client-host #f)
                                  (if (<= (hash-ref clients-table client-host) 1)
                                      (hash-remove! clients-table client-host)
                                      (hash-set! clients-table client-host (sub1 (hash-ref clients-table client-host)))))
                                (kill-current-ftp-process)
                                (close-output-port *client-output-port*);ssl required
                                (custodian-shutdown-all cust))))
                    (begin
                      (print-crlf/encoding** 'MAX-CLIENTS-PER-IP)
                      (close-output-port *client-output-port*);ssl required
                      (close-input-port *client-input-port*)
                      (next-accept)))))))))
    ;;
    ;; ---------- Private Methods ----------
    ;;
    (define (accept-client-request no-transfer-time)
      (with-handlers ([any/c debug/handler])
        (let-values ([(start-timer pause-timer reset-timer kill-timer)
                      (alarm-clock 1 no-transfer-time
                                   (λ()
                                     (print-crlf/encoding** 'TRANSFER-WAIT-TIME)
                                     (*quit*)))])
          (do ([p (regexp-split #rx"\n|\r" welcome-message) (cdr p)])
            [(null? (cdr p))
             (printf-crlf/encoding *locale-encoding* *client-output-port* "220 ~a" (car p))]
            (printf-crlf/encoding *locale-encoding* *client-output-port* "220-~a" (car p)))
          (start-timer)
          (let loop ([request (read-request *locale-encoding* *client-input-port*)])
            (unless (eof-object? request)
              (pause-timer) 
              (reset-timer)
              (if (regexp-match? #rx"[^ ]+" request)
                  (let* ([cmd (string-upcase (car (regexp-match #rx"[^ ]+" request)))]
                         [cmdinfo (hash-ref *cmd-voc* cmd #f)])
                    (if cmdinfo
                        (with-handlers ([exn:fail:filesystem? 
                                         (λ (e) 
                                           (when-drdebug (displayln e))
                                           (print-crlf/encoding* "550 System error."))]
                                        [any/c (λ(e) 
                                                 (when-drdebug (displayln e))
                                                 (print-crlf/encoding** 'UNKNOWN-ERROR))])
                          (if (or *userstruct* (car cmdinfo))
                              ((cadr cmdinfo) (get-params request))
                              (print-crlf/encoding** 'PLEASE-LOGIN)))
                        (print-crlf/encoding** 'CMD-NOT-IMPLEMENTED cmd)))
                  (print-crlf/encoding** 'INVALID-CMD-SYNTAX))
              (start-timer)
              (sleep .005)
              (loop (read-request *locale-encoding* *client-input-port*)))))))
    
    (define (USER-COMMAND params)
      (if params
          (let ([name params])
            (if (and (userinfo/login users-table name)
                     (ftp-user-anonymous? (userinfo/login users-table name)))
                (print-crlf/encoding** 'ANONYMOUS-LOGIN)
                (print-crlf/encoding** 'PASSW-REQUIRED name))
            (set! *login* name))
          (begin
            (print-crlf/encoding** 'SYNTAX-ERROR "")
            (set! *login* #f)))
      (set! *userstruct* #f))
    
    (define (PASS-COMMAND params)
      (sleep pass-sleep-sec)
      (let ([correct
             (if (string? *login*)
                 (let ([pass params])
                   (cond
                     ((not (userinfo/login users-table *login*))
                      'login-incorrect)
                     ((ftp-user-anonymous? (userinfo/login users-table *login*))
                      'anonymous-logged-in)
                     ((and (hash-ref bad-auth-table *login* #f)
                           ((mcar (hash-ref bad-auth-table *login*)). >= . max-auth-attempts)
                           (<= ((current-seconds). - .(mcdr (hash-ref bad-auth-table *login*)))
                               bad-auth-sleep-sec))
                      (let ([pair (hash-ref bad-auth-table *login*)])
                        (set-mcar! pair (add1 (mcar pair)))
                        (set-mcdr! pair (current-seconds)))
                      'login-incorrect)
                     ((not pass)
                      'login-incorrect)
                     ((check-user-pass (ftp-user-real-user (userinfo/login users-table *login*)) pass)
                      (when (hash-ref bad-auth-table *login* #f)
                        (hash-remove! bad-auth-table *login*))
                      'user-logged-in)
                     (else
                      (if (hash-ref bad-auth-table *login* #f)
                          (let ([pair (hash-ref bad-auth-table *login*)])
                            (set-mcar! pair (add1 (mcar pair)))
                            (set-mcdr! pair (current-seconds)))
                          (hash-set! bad-auth-table *login* (mcons 1 (current-seconds))))
                      'passw-incorrect)))
                 'login-incorrect)])
        (case correct
          [(user-logged-in)
           (set! *root-dir* (ftp-user-root-dir (userinfo/login users-table *login*)))
           (if (directory-exists? *root-dir*)
               (begin
                 (set! *userstruct* (userinfo/login users-table *login*))
                 (print-log-event "User logged in.")
                 (print-crlf/encoding** 'USER-LOGGED *login*))
               (begin
                 (set! *userstruct* #f)
                 (print-log-event "Users-config file incorrect.")
                 (print-crlf/encoding** 'LOGIN-INCORRECT)))]
          [(anonymous-logged-in)
           (set! *root-dir* (ftp-user-root-dir (userinfo/login users-table *login*)))
           (if (directory-exists? *root-dir*)
               (begin
                 (set! *userstruct* (userinfo/login users-table *login*))
                 (print-log-event "Anonymous user logged in.")
                 (print-crlf/encoding** 'ANONYMOUS-LOGGED))
               (begin
                 (set! *userstruct* #f)
                 (print-log-event "Users-config file incorrect.")
                 (print-crlf/encoding** 'LOGIN-INCORRECT)))]
          [(login-incorrect)
           (set! *userstruct* #f)
           (print-log-event "Login incorrect.")
           (print-crlf/encoding** 'LOGIN-INCORRECT)]
          [(passw-incorrect)
           (set! *userstruct* #f)
           (print-log-event "Password incorrect.")
           (print-crlf/encoding** 'LOGIN-INCORRECT)])))
    
    (define (REIN-COMMAND params)
      (if params
          (print-crlf/encoding** 'SYNTAX-ERROR "")
          (begin
            (set! *login* #f)
            (set! *userstruct* #f)
            (print-crlf/encoding** 'SERVICE-READY))))
    
    (define (QUIT-COMMAND params)
      (if params
          (print-crlf/encoding** 'SYNTAX-ERROR "")
          (begin
            (print-crlf/encoding** 'QUIT)
            (*quit*))))
    
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
                 (print-crlf/encoding* 
                  (format " MLST Type~a;Size~a;Modify~a;Perm~a;Unique~a;Charset~a;UNIX.mode~a;UNIX.owner~a;UNIX.group~a;"
                          (if (ftp-mlst-features-type? *mlst-features*) "*" "")
                          (if (ftp-mlst-features-size? *mlst-features*) "*" "")
                          (if (ftp-mlst-features-modify? *mlst-features*) "*" "")
                          (if (ftp-mlst-features-perm? *mlst-features*) "*" "")
                          (if (ftp-mlst-features-unique? *mlst-features*) "*" "")
                          (if (ftp-mlst-features-charset? *mlst-features*) "*" "")
                          (if (ftp-mlst-features-unix-mode? *mlst-features*) "*" "")
                          (if (ftp-mlst-features-unix-owner? *mlst-features*) "*" "")
                          (if (ftp-mlst-features-unix-group? *mlst-features*) "*" ""))))
            (and (hash-ref *cmd-voc* "MLSD" #f)
                 (print-crlf/encoding* " MLSD"))
            (and (hash-ref *cmd-voc* "SIZE" #f)
                 (print-crlf/encoding* " SIZE"))
            (and (hash-ref *cmd-voc* "MDTM" #f)
                 (print-crlf/encoding* " MDTM"))
            (and (hash-ref *cmd-voc* "MFMT" #f)
                 (print-crlf/encoding* " MFMT"))
            (and (hash-ref *cmd-voc* "MFF" #f)
                 (print-crlf/encoding* " MFF Modify;UNIX.mode;UNIX.owner;UNIX.group;"))
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
    
    (define (PORT-COMMAND params)
      (if (and params
               (regexp-match #rx"^[0-9]+,[0-9]+,[0-9]+,[0-9]+,[0-9]+,[0-9]+$" params))
          (let* ([l (string-split-char #\, params)]
                 [delzero (λ(snum) (number->string (string->number snum)))]
                 [host (string-append (delzero (first l)) "." (delzero (second l)) "."
                                      (delzero (third l)) "." (delzero (fourth l)))]
                 [port (((string->number (fifth l)). * . 256). + .(string->number (sixth l)))])
            (if (and (IPv4? host) (port-number? port))
                (if (and (port . > . 1024)
                         (or allow-foreign-address
                             (private-IPv4? host)
                             (string=? host *client-host*)))
                    (begin
                      (set! DTP 'active)
                      ;(set! active-host&port (ftp-host&port host port))
                      (set! active-host&port (ftp-host&port (if (private-IPv4? host) *client-host* host) port))
                      (print-crlf/encoding** 'CMD-SUCCESSFUL 200 "PORT"))
                    (print-crlf/encoding** 'FORBIDDEN-ADDR-PORT))
                (print-crlf/encoding** 'SYNTAX-ERROR "")))
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    (define (PASV-COMMAND params)
      (if params
          (print-crlf/encoding** 'SYNTAX-ERROR "")
          (if (eq? *current-protocol* '|1|)
              (let*-values ([(psv-port) (+ passive-ports-from
                                           (random (- passive-ports-to passive-ports-from -1)
                                                   random-gen))]
                            [(h1 h2 h3 h4) (apply values (regexp-split #rx"\\." passive-host))]
                            [(p1 p2) (quotient/remainder psv-port 256)])
                (set! DTP 'passive)
                (set! passive-host&port (ftp-host&port passive-host psv-port))
                (kill-current-ftp-process)
                (set! current-process (make-custodian))
                (parameterize ([current-custodian current-process])
                  (set! pasv-listener (tcp-listen psv-port 1 #t server-host)))
                (print-crlf/encoding** 'PASV h1 h2 h3 h4 p1 p2))
              (print-crlf/encoding** 'BAD-PROTOCOL))))
    
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
            (print-crlf/encoding** 'STATUS-INFO-3 *login*)
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
                       (if (andmap (λ (mode) (member mode '(TYPE SIZE MODIFY PERM UNIQUE CHARSET 
                                                                 UNIX.MODE UNIX.OWNER UNIX.GROUP)))
                                   mlst)
                           (begin
                             (set! *mlst-features* (ftp-mlst-features #f #f #f))
                             (for-each (λ (mode)
                                         (case mode
                                           [(TYPE) (set-ftp-mlst-features-type?! *mlst-features* #t)]
                                           [(SIZE) (set-ftp-mlst-features-size?! *mlst-features* #t)]
                                           [(MODIFY) (set-ftp-mlst-features-modify?! *mlst-features* #t)]
                                           [(PERM) (set-ftp-mlst-features-perm?! *mlst-features* #t)]
                                           [(UNIQUE) (set-ftp-mlst-features-unique?! *mlst-features* #t)]
                                           [(CHARSET) (set-ftp-mlst-features-charset?! *mlst-features* #t)]
                                           [(UNIX.MODE) (set-ftp-mlst-features-unix-mode?! *mlst-features* #t)]
                                           [(UNIX.OWNER) (set-ftp-mlst-features-unix-owner?! *mlst-features* #t)]
                                           [(UNIX.GROUP) (set-ftp-mlst-features-unix-group?! *mlst-features* #t)]))
                                       mlst)
                             (print-crlf/encoding** 'MLST-ON))
                           (print-crlf/encoding** 'SYNTAX-ERROR "MLST:")))
                     (print-crlf/encoding** 'SYNTAX-ERROR "MLST:"))))
              (else (print-crlf/encoding** 'SYNTAX-ERROR ""))))
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    
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
              (if (and (port . > . 1024)
                       (or allow-foreign-address
                           (and (= prt 1)
                                (private-IPv4? ip))
                           (string=? ip *client-host*)))
                  (begin
                    (set! DTP 'active)
                    (set! active-host&port (ftp-host&port (if (and (= prt 1) 
                                                                   (private-IPv4? ip))
                                                              *client-host* 
                                                              ip)
                                                          port))
                    (print-crlf/encoding** 'CMD-SUCCESSFUL 200 "EPRT"))
                  (print-crlf/encoding** 'FORBIDDEN-ADDR-PORT))))
          (print-crlf/encoding** 'SYNTAX-ERROR "EPRT:")))
    
    (define (EPSV-COMMAND params)
      (local [(define (set-psv)
                (set! DTP 'passive)
                (let ([psv-port (+ passive-ports-from
                                   (random (- passive-ports-to passive-ports-from -1)
                                           random-gen))])
                  (set! passive-host&port (ftp-host&port passive-host psv-port))
                  (kill-current-ftp-process)
                  (set! current-process (make-custodian))
                  (parameterize ([current-custodian current-process])
                    (set! pasv-listener (tcp-listen psv-port 1 #t server-host)))
                  (print-crlf/encoding** 'EPSV psv-port)))
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
              (case (string->symbol (string-upcase (car (regexp-match #rx"^1|2|[aA][lL][lL]$" params))))
                [(|1|) (epsv-1)]
                [(|2|) (epsv-2)]
                [(ALL) (epsv)]))
            (epsv))))
    
    (define (HELP-COMMAND params)
      (if params
          (with-handlers ([any/c (λ (e) (print-crlf/encoding** 'UNKNOWN-CMD params))])
            (print-crlf/encoding** 'HELP (cddr (hash-ref *cmd-voc* (string-upcase params)))))
          (begin
            (print-crlf/encoding** 'HELP-LISTING)
            (for-each (λ (rec) (print-crlf/encoding* (format " ~a" (car rec)))) *cmd-list*)
            (print-crlf/encoding** 'END 214))))
    
    (define (CWD-COMMAND params)
      (if params
          (let ([spath (build-ftp-spath* params)])
            (if (ftp-dir-execute-ok? spath)
                (begin
                  (set! *current-dir* (simplify-ftp-path spath))
                  (print-crlf/encoding** 'CMD-SUCCESSFUL 250 "CWD"))
                (print-crlf/encoding** 'DIR-NOT-FOUND)))
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
         
         (define (read-stat ftp-path)
           (let* ([full-path (string-append *root-dir* ftp-path)]
                  [stat (ftp-lstat full-path)]
                  [mode (Stat-mode stat)]
                  [owner (uid->uname (Stat-uid stat))]
                  [group (gid->gname (Stat-gid stat))])
             (format "~a~a~a~a ~3F ~8F ~8F ~14F ~a" 
                     (cond
                       [(bitwise-bit-set? mode 15)
                        (cond
                          [(bitwise-bit-set? mode 13) "l"]
                          [(bitwise-bit-set? mode 14) "s"]
                          [else "-"])]
                       [(bitwise-bit-set? mode 14)
                        (if (bitwise-bit-set? mode 13) "b" "d")]
                       [(bitwise-bit-set? mode 13) "c"]
                       [(bitwise-bit-set? mode 12) "p"]
                       [else "-"])
                     (string (if (bitwise-bit-set? mode 8) #\r #\-)
                             (if (bitwise-bit-set? mode 7) #\w #\-)
                             (if (bitwise-bit-set? mode 11)
                                 (if (bitwise-bit-set? mode 6) #\s #\S)
                                 (if (bitwise-bit-set? mode 6) #\x #\-)))
                     (string (if (bitwise-bit-set? mode 5) #\r #\-)
                             (if (bitwise-bit-set? mode 4) #\w #\-)
                             (if (bitwise-bit-set? mode 10)
                                 (if (bitwise-bit-set? mode 3) #\s #\S)
                                 (if (bitwise-bit-set? mode 3) #\x #\-)))
                     (string (if (bitwise-bit-set? mode 2) #\r #\-)
                             (if (bitwise-bit-set? mode 1) #\w #\-)
                             (if (bitwise-bit-set? mode 9)
                                 (if (bitwise-bit-set? mode 0) #\t #\T)
                                 (if (bitwise-bit-set? mode 0) #\x #\-)))
                     (Stat-nlink stat)
                     (or owner (Stat-uid stat))
                     (or group (Stat-gid stat))
                     (Stat-size stat)
                     (date-time->string (seconds->date (Stat-mtime stat))))))
         
         (define (dlst ftp-dir)
           (if (ftp-dir-execute-ok? ftp-dir)
               (let* ([full-dir (string-append *root-dir* ftp-dir)]
                      [dirlist 
                       (if (ftp-dir-list-ok? ftp-dir)
                           (string-append*
                            (if short?
                                ".\n"
                                (string-append (read-stat ftp-dir) " .\n"))
                            (if short?
                                "..\n"
                                (string-append (if (= (file-or-directory-identity full-dir)
                                                      (file-or-directory-identity *root-dir*))
                                                   (read-stat ftp-dir)
                                                   (read-stat (simplify-ftp-path ftp-dir 1)))
                                               " ..\n"))
                            (map (λ (p)
                                   (let* ([spath (path->string p)]
                                          [ftp-spath (string-append ftp-dir "/" spath)])
                                     (if (ftp-obj-exists? ftp-spath)
                                         (if short?
                                             (string-append spath "\n")
                                             (string-append (read-stat ftp-spath) " " spath "\n"))
                                         "")))
                                 (directory-list full-dir)))
                           "")])
                 (if status
                     (print-crlf/encoding* dirlist)
                     (ftp-data-transfer (case representation-type
                                          ((ASCII) dirlist)
                                          ((Image) (string->bytes/encoding *locale-encoding* dirlist))))))
               (unless status
                 (print-crlf/encoding** 'DIR-NOT-FOUND))))]
        
        (let ([dir (if (and params (eq? (string-ref params 0) #\-))
                       (let ([d (regexp-match #px"[^-\\w][^ \t-]+.*" params)])
                         (and d (substring (car d) 1)))
                       params)])
          (if dir
              (dlst (build-ftp-spath* dir))
              (dlst *current-dir*)))))
    
    (define (MLST-COMMAND params)
      (local [(define (mlst ftp-path)
                (let ([path (string-append *root-dir* ftp-path)])
                  (if (and (ftp-dir-list-ok? (simplify-ftp-path ftp-path 1))
                           (not (= (file-or-directory-identity path)
                                   (file-or-directory-identity *root-dir*))))
                      (begin
                        (print-crlf/encoding** 'MLST-LISTING)
                        (print-crlf/encoding* (string-append " " (mlst-info ftp-path)))
                        (print-crlf/encoding** 'END 250))
                      (print-crlf/encoding** 'FILE-DIR-NOT-FOUND))))]
        (if params
            (let ([spath (build-ftp-spath* params)])
              (if (ftp-obj-exists? spath)
                  (mlst spath)
                  (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)))
            (if (ftp-dir-execute-ok? *current-dir*)
                (mlst *current-dir*)
                (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)))))
    
    (define (MLSD-COMMAND params)
      (local [(define (mlsd ftp-path)
                (if (ftp-dir-execute-ok? ftp-path)
                    (let* ([path (string-append *root-dir* ftp-path)]
                           [dirlist 
                            (if (ftp-dir-list-ok? ftp-path)
                                (string-append*
                                 (string-append (mlst-info ftp-path #f ".") "\n")
                                 (string-append (if (= (file-or-directory-identity path)
                                                       (file-or-directory-identity *root-dir*))
                                                    (mlst-info ftp-path #f "..")
                                                    (mlst-info (string-append ftp-path "/..") #f ".."))
                                                "\n")
                                 (map (λ (p)
                                        (let* ([spath (path->string p)]
                                               [ftp-spath (string-append ftp-path "/" spath)])
                                          (if (ftp-obj-exists? ftp-spath)
                                              (string-append (mlst-info ftp-spath #f) "\n")
                                              "")))
                                      (directory-list path)))
                                "")])
                      (ftp-data-transfer (case representation-type
                                           ((ASCII) dirlist)
                                           ((Image) (string->bytes/encoding *locale-encoding* dirlist)))))
                    (print-crlf/encoding** 'DIR-NOT-FOUND)))]
        (if params
            (mlsd (build-ftp-spath* params))
            (mlsd *current-dir*))))
    
    (define (MKD-COMMAND params)
      (if params
          (if current-user-mkdir-ok?
              (let* ([spath (let ([p (build-ftp-spath* params)]) (deltail/ p))]
                     [full-spath (string-append *root-dir* spath)])
                (with-handlers ([exn:posix? (λ (e)
                                              (case (exn:posix-errno e)
                                                [(1 13) (print-crlf/encoding** 'CREATE-DIR-PERM-DENIED)]
                                                [(17) (print-crlf/encoding** 'DIR-EXIST)]
                                                [(2) (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)]
                                                [else (print-crlf/encoding** 'CANT-CREATE-DIR)]))])
                  (ftp-mkdir full-spath (ftp-user-uid *userstruct*) (ftp-user-gid *userstruct*))
                  (print-log-event (format "Make directory ~a" (simplify-ftp-path spath)))
                  (print-crlf/encoding** 'DIR-CREATED (simplify-ftp-path spath))))
              (print-crlf/encoding** 'CREATE-DIR-PERM-DENIED))
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    (define (RMD-COMMAND params)
      (if params
          (if current-user-delete-ok?
              (let* ([spath (let ([p (build-ftp-spath* params)]) (deltail/ p))]
                     [full-spath (string-append *root-dir* spath)])
                (if (= (file-or-directory-identity full-spath)
                       (file-or-directory-identity *root-dir*))
                    (print-crlf/encoding** 'DIR-NOT-FOUND)
                    (with-handlers ([exn:posix? (λ (e)
                                                  (case (exn:posix-errno e)
                                                    [(1 13) (print-crlf/encoding** 'DELDIR-PERM-DENIED)]
                                                    [(39) (print-crlf/encoding** 'DELDIR-NOT-EMPTY)]
                                                    [(20 2) (print-crlf/encoding** 'DIR-NOT-FOUND)]
                                                    [else (print-crlf/encoding** 'CANT-RMDIR)]))])
                      (ftp-rmdir* *userstruct* full-spath)
                      (print-log-event (format "Remove a directory ~a" (simplify-ftp-path spath)))
                      (print-crlf/encoding** 'CMD-SUCCESSFUL 250 "RMD"))))
              (print-crlf/encoding** 'DELDIR-PERM-DENIED))
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    (define (APPE-COMMAND params)
      (if params
          (if current-user-store&append-ok?
              (let* ([spath (build-ftp-spath* params)]
                     [full-spath (string-append *root-dir* spath)])
                (if (file-exists? full-spath)
                    (if (ftp-access-ok? spath 2)
                        (ftp-store-file *userstruct* full-spath 'append)
                        (print-crlf/encoding** 'APPEND-FILE-PERM-DENIED))
                    (print-crlf/encoding** 'FILE-NOT-FOUND)))
              (print-crlf/encoding** 'APPEND-FILE-PERM-DENIED))
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    (define (STOR-COMMAND params)
      (if params
          (if current-user-store&append-ok?
              (let* ([spath (build-ftp-spath* params)]
                     [full-spath (string-append *root-dir* spath)]
                     [parent (simplify-ftp-path spath 1)])
                (if (directory-exists? (string-append *root-dir* parent))
                    (if (ftp-access-ok? parent 3)
                        (ftp-store-file *userstruct* full-spath 'truncate)
                        (print-crlf/encoding** 'STORE-FILE-PERM-DENIED))
                    (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)))
              (print-crlf/encoding** 'STORE-FILE-PERM-DENIED))
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    (define (STOU-COMMAND params)
      (cond
        (params
         (print-crlf/encoding** 'SYNTAX-ERROR ""))
        ((ftp-dir-store-ok? *current-dir*)
         (let* ([file-name (let loop ([fname (gensym "noname")])
                             (if (file-exists? (string-append *root-dir* *current-dir* "/" fname))
                                 (loop (gensym "noname"))
                                 fname))]
                [path (string-append *root-dir* *current-dir* "/" file-name)])
           (ftp-store-file *userstruct* path 'truncate)))
        (else
         (print-crlf/encoding** 'STORE-FILE-PERM-DENIED))))
    
    (define (RETR-COMMAND params)
      (if params
          (let* ([spath (build-ftp-spath* params)]
                 [full-spath (string-append *root-dir* spath)])
            (if (file-exists? full-spath)
                (if (ftp-access-ok? spath 4)
                    (ftp-data-transfer full-spath #t)
                    (print-crlf/encoding** 'PERM-DENIED))
                (print-crlf/encoding** 'FILE-NOT-FOUND)))
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    (define (DELE-COMMAND params)
      (if params
          (if current-user-delete-ok?
              (with-handlers ([exn:posix? (λ (e)
                                            (case (exn:posix-errno e)
                                              [(1 13) (print-crlf/encoding** 'DELFILE-PERM-DENIED)]
                                              [(2 20 21) (print-crlf/encoding** 'FILE-NOT-FOUND)]
                                              [else (print-crlf/encoding** 'CMD-FAILED 550 "DELE")]))])
                (let ([spath (build-ftp-spath* params)])
                  (ftp-unlink* *userstruct* (string-append *root-dir* spath))
                  (print-log-event (format "Delete a file ~a" (simplify-ftp-path spath)))
                  (print-crlf/encoding** 'CMD-SUCCESSFUL 250 "DELE")))
              (print-crlf/encoding** 'DELFILE-PERM-DENIED))
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    (define (SIZE-COMMAND params)
      (if params
          (if current-user-list-ok?
              (with-handlers ([exn:posix? (λ (e)
                                            (case (exn:posix-errno e)
                                              [(13) (print-crlf/encoding** 'PERM-DENIED)]
                                              [(2 20) (print-crlf/encoding** 'FILE-NOT-FOUND)]
                                              [else (print-crlf/encoding** 'CMD-FAILED 550 "SIZE")]))])
                (let* ([spath (build-ftp-spath* params)]
                       [stat (ftp-stat* *userstruct* (string-append *root-dir* spath))])
                  (if (= (bitwise-and (Stat-mode stat) 57344) 32768)
                      (print-crlf/encoding* (format "213 ~a" (Stat-size stat)))
                      (print-crlf/encoding** 'FILE-NOT-FOUND))))
              (print-crlf/encoding** 'PERM-DENIED))
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    (define (MDTM-COMMAND params)
      (if params
          (if current-user-list-ok?
              (with-handlers ([exn:posix? (λ (e)
                                            (case (exn:posix-errno e)
                                              [(13) (print-crlf/encoding** 'PERM-DENIED)]
                                              [(2 20) (print-crlf/encoding** 'FILE-NOT-FOUND)]
                                              [else (print-crlf/encoding** 'CMD-FAILED 550 "MDTM")]))])
                (let* ([spath (build-ftp-spath* params)]
                       [stat (ftp-stat* *userstruct* (string-append *root-dir* spath))])
                  (if (= (bitwise-and (Stat-mode stat) 57344) 32768)
                      (print-crlf/encoding* (format "213 ~a" (seconds->mdtm-time-format (Stat-mtime stat))))
                      (print-crlf/encoding** 'FILE-NOT-FOUND))))
              (print-crlf/encoding** 'PERM-DENIED))
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    (define (RNFR-COMMAND params)
      (if params
          (if current-user-rename-ok?
              (let* ([spath (build-ftp-spath* params)]
                     [full-spath (string-append *root-dir* spath)])
                (if (and (ftp-stat full-spath)
                         (not (= (file-or-directory-identity full-spath)
                                 (file-or-directory-identity *root-dir*))))
                    (if (ftp-access-ok? (simplify-ftp-path spath 1) 3)
                        (begin
                          (set! *rename-path* full-spath)
                          (print-crlf/encoding** 'RENAME-OK))
                        (print-crlf/encoding** 'PERM-DENIED))
                    (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)))
              (print-crlf/encoding** 'PERM-DENIED))
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    (define (RNTO-COMMAND params)
      (if params
          (if (and current-user-rename-ok? *rename-path*)
              (let* ([new-path (let ([p (build-ftp-spath* params)]) (deltail/ p))]
                     [parent-new (and (path-string? new-path)
                                      (path->string (path-only new-path)))])
                (if (and parent-new 
                         (ftp-stat (string-append *root-dir* parent-new)))
                    (if (ftp-access-ok? parent-new 3)
                        (if (ftp-stat (string-append *root-dir* new-path))
                            (print-crlf/encoding** 'CANT-RENAME-EXIST)
                            (with-handlers ([exn:fail:filesystem?
                                             (λ (e) (print-crlf/encoding** 'CANT-RENAME))])
                              (rename-file-or-directory *rename-path* (string-append *root-dir* new-path))
                              (print-log-event (format "Rename the file or directory from ~a to ~a"
                                                       (real-path->ftp-path *rename-path* *root-dir*)
                                                       (simplify-ftp-path new-path)))
                              (print-crlf/encoding** 'CMD-SUCCESSFUL 250 "RNTO")))
                        (print-crlf/encoding** 'RENAME-PERM-DENIED))
                    (print-crlf/encoding** 'FILE-DIR-NOT-FOUND))
                (set! *rename-path* #f))
              (print-crlf/encoding** 'RENAME-PERM-DENIED))
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    (define (MFMT-COMMAND params)
      (if (and params (regexp-match? #rx"^[0-9]+[ ]+.+" params))
          (if current-user-store&append-ok?
              (let* ([mt (car (regexp-match #rx"[0-9]+" params))]
                     [modtime (mdtm-time-format->seconds mt)]
                     [ftp-path (let ([p (get-params params)])
                                 (and p (build-ftp-spath* p)))])
                (if (and modtime ftp-path)
                    (with-handlers ([exn:posix? (λ (e)
                                                  (case (exn:posix-errno e)
                                                    [(1 13) (print-crlf/encoding** 'PERM-DENIED)]
                                                    [(2) (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)]
                                                    [else (print-crlf/encoding** 'CANT-MFMT)]))])
                      (ftp-utime* *userstruct* (string-append *root-dir* ftp-path) modtime modtime)
                      (print-log-event (format "Modify a last modification time of ~s in ~a"
                                               (simplify-ftp-path ftp-path) mt))
                      (print-crlf/encoding** 'MFMT-OK mt (simplify-ftp-path ftp-path)))
                    (print-crlf/encoding** 'SYNTAX-ERROR "")))
              (print-crlf/encoding** 'PERM-DENIED))
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    (define (MFF-COMMAND params)
      (call/cc 
       (λ (return)
         (local [(define (change-mtime mt ftp-path)
                   (let ([modtime (mdtm-time-format->seconds mt)])
                     (if modtime
                         (with-handlers ([exn:posix? (λ (e)
                                                       (case (exn:posix-errno e)
                                                         [(1 13) (print-crlf/encoding** 'PERM-DENIED)]
                                                         [(2) (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)]
                                                         [else (print-crlf/encoding** 'CMD-FAILED 550 "MFF")])
                                                       (return))])
                           (ftp-utime* *userstruct* (string-append *root-dir* ftp-path) modtime modtime)
                           (print-log-event (format "Modify a last modification time of ~s in ~a"
                                                    (simplify-ftp-path ftp-path) mt)))
                         (return (print-crlf/encoding** 'SYNTAX-ERROR "MODIFY:")))))
                 
                 (define (change-mode mode ftp-path)
                   (with-handlers ([exn:posix? (λ (e)
                                                 (case (exn:posix-errno e)
                                                   [(1 13) (print-crlf/encoding** 'PERM-DENIED)]
                                                   [(2 20) (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)]
                                                   [else (print-crlf/encoding** 'CMD-FAILED 550 "MFF")])
                                                 (return))])
                     (ftp-chmod* *userstruct* (string-append *root-dir* ftp-path) mode)
                     (print-log-event (format "Change the permissions of a ~a" 
                                              (simplify-ftp-path ftp-path)))))
                 
                 (define (change-owner owner ftp-path)
                   (let ([uid (if (regexp-match? #rx"^[0-9]+$" owner)
                                  (let ([uid (string->number owner)])
                                    (and (>= uid 0)
                                         (< uid #xffffffff)
                                         uid))
                                  (login->uid owner))])
                     (if uid
                         (with-handlers ([exn:posix? (λ (e)
                                                       (case (exn:posix-errno e)
                                                         [(1 13) (print-crlf/encoding** 'PERM-DENIED)]
                                                         [(2 20) (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)]
                                                         [else (print-crlf/encoding** 'CMD-FAILED 550 "MFF")])
                                                       (return))])
                           (ftp-chown* *userstruct* (string-append *root-dir* ftp-path) uid #xffffffff)
                           (print-log-event (format "Change the owner of a ~a" 
                                                    (simplify-ftp-path ftp-path))))
                         (return (print-crlf/encoding** 'PERM-DENIED)))))
                 
                 (define (change-group group ftp-path)
                   (let ([gid (if (regexp-match? #rx"^[0-9]+$" group)
                                  (let ([gid (string->number group)])
                                    (and (>= gid 0)
                                         (< gid #xffffffff)
                                         gid))
                                  (gname->gid group))])
                     (if gid
                         (with-handlers ([exn:posix? (λ (e)
                                                       (case (exn:posix-errno e)
                                                         [(1 13) (print-crlf/encoding** 'PERM-DENIED)]
                                                         [(2 20) (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)]
                                                         [else (print-crlf/encoding** 'CMD-FAILED 550 "MFF")])
                                                       (return))])
                           (ftp-chown* *userstruct* (string-append *root-dir* ftp-path) #xffffffff gid)
                           (print-log-event (format "Change the group of a ~a" 
                                                    (simplify-ftp-path ftp-path))))
                         (return (print-crlf/encoding** 'PERM-DENIED)))))]
           (let ([modify #f]
                 [unix-mode #f]
                 [unix-owner #f]
                 [unix-group #f])
             (if (and params (regexp-match? #rx"^([A-z\\.]+=[^ \t]+;)+[ ]+.+" params))
                 (if current-user-store&append-ok?
                     (let* ([lst (string-split-char #\; params)]
                            [path (delete-lws (last lst))]
                            [facts (remove (last lst) lst)])
                       (if (and (pair? facts) (path-string? path))
                           (let ([ftp-path (build-ftp-spath* path)])
                             (for-each (λ (f)
                                         (let ([l (string-split-char #\= f)])
                                           (case (string->symbol (string-upcase (car l)))
                                             [(MODIFY)
                                              (if (and (not modify) (regexp-match? #rx"^[0-9]+$" (cadr l)))
                                                  (set! modify (cadr l))
                                                  (return (print-crlf/encoding** 'SYNTAX-ERROR "MODIFY:")))]
                                             [(UNIX.MODE) 
                                              (if (and (not unix-mode) (regexp-match? #rx"^[0-7]?[0-7][0-7][0-7]$" (cadr l)))
                                                  (set! unix-mode (cadr l))
                                                  (return (print-crlf/encoding** 'SYNTAX-ERROR "UNIX.MODE:")))]
                                             [(UNIX.OWNER) 
                                              (if (and (not unix-owner) (regexp-match? #rx"^[^ ]+$" (cadr l)))
                                                  (set! unix-owner (cadr l))
                                                  (return (print-crlf/encoding** 'SYNTAX-ERROR "UNIX.OWNER:")))]
                                             [(UNIX.GROUP) 
                                              (if (and (not unix-group) (regexp-match? #rx"^[^ ]+$" (cadr l)))
                                                  (set! unix-group (cadr l))
                                                  (return (print-crlf/encoding** 'SYNTAX-ERROR "UNIX.GROUP:")))]
                                             [else (return (print-crlf/encoding** 'SYNTAX-ERROR ""))])))
                                       facts)
                             (when modify (change-mtime modify ftp-path))
                             (when unix-mode (change-mode (string->number unix-mode 8) ftp-path))
                             (when unix-owner (change-owner unix-owner ftp-path))
                             (when unix-group (change-group unix-group ftp-path))
                             (print-crlf/encoding** 'MFF-OK
                                                    (string-append (if modify (format "Modify=~a;" modify) "")
                                                                   (if unix-mode (format "UNIX.mode=~a;" unix-mode) "")
                                                                   (if unix-owner (format "UNIX.owner=~a;" unix-owner) "")
                                                                   (if unix-group (format "UNIX.group=~a;" unix-group) ""))
                                                    (simplify-ftp-path ftp-path)))
                           (print-crlf/encoding** 'SYNTAX-ERROR "")))
                     (print-crlf/encoding** 'PERM-DENIED))
                 (print-crlf/encoding** 'SYNTAX-ERROR "")))))))
    
    (define (SITE-COMMAND params)
      (local
        [(define (chmod mode ftp-path)
           (with-handlers ([exn:posix? (λ (e)
                                         (case (exn:posix-errno e)
                                           [(1 13) (print-crlf/encoding** 'PERM-DENIED)]
                                           [(2 20) (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)]
                                           [else (print-crlf/encoding** 'CMD-FAILED 550 "SITE CHMOD")]))])
             (ftp-chmod* *userstruct* (string-append *root-dir* ftp-path) mode)
             (print-log-event (format "Change the permissions of a ~a" 
                                      (simplify-ftp-path ftp-path)))
             (print-crlf/encoding** 'CMD-SUCCESSFUL 200 "SITE CHMOD")))
         
         (define (chown owner ftp-path)
           (let ([uid (if (regexp-match? #rx"^[0-9]+$" owner)
                          (let ([uid (string->number owner)])
                            (and (>= uid 0)
                                 (< uid #xffffffff)
                                 uid))
                          (login->uid owner))])
             (if uid
                 (with-handlers ([exn:posix? (λ (e)
                                               (case (exn:posix-errno e)
                                                 [(1 13) (print-crlf/encoding** 'PERM-DENIED)]
                                                 [(2 20) (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)]
                                                 [else (print-crlf/encoding** 'CMD-FAILED 550 "SITE CHOWN")]))])
                   (ftp-chown* *userstruct* (string-append *root-dir* ftp-path) uid #xffffffff)
                   (print-log-event (format "Change the owner of a ~a" 
                                            (simplify-ftp-path ftp-path)))
                   (print-crlf/encoding** 'CMD-SUCCESSFUL 200 "SITE CHOWN"))
                 (print-crlf/encoding** 'PERM-DENIED))))
         
         (define (chgrp group ftp-path)
           (let ([gid (if (regexp-match? #rx"^[0-9]+$" group)
                          (let ([gid (string->number group)])
                            (and (>= gid 0)
                                 (< gid #xffffffff)
                                 gid))
                          (gname->gid group))])
             (if gid
                 (with-handlers ([exn:posix? (λ (e)
                                               (case (exn:posix-errno e)
                                                 [(1 13) (print-crlf/encoding** 'PERM-DENIED)]
                                                 [(2 20) (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)]
                                                 [else (print-crlf/encoding** 'CMD-FAILED 550 "SITE CHGRP")]))])
                   (ftp-chown* *userstruct* (string-append *root-dir* ftp-path) #xffffffff gid)
                   (print-log-event (format "Change the group of a ~a" 
                                            (simplify-ftp-path ftp-path)))
                   (print-crlf/encoding** 'CMD-SUCCESSFUL 200 "SITE CHGRP"))
                 (print-crlf/encoding** 'PERM-DENIED))))]
        
        (if params
            (if current-user-store&append-ok?
                (let ([cmd (string->symbol (string-upcase (car (regexp-match #rx"[^ ]+" params))))])
                  (case cmd
                    [(CHMOD)
                     (let* ([permis+path (get-params params)]
                            [permis (regexp-match #rx"^[0-7]?[0-7][0-7][0-7]" permis+path)])
                       (if permis
                           (let ([path (get-params permis+path)])
                             (if path
                                 (chmod (string->number (car permis) 8) (build-ftp-spath* path))
                                 (print-crlf/encoding** 'SYNTAX-ERROR "CHMOD:")))
                           (print-crlf/encoding** 'SYNTAX-ERROR "CHMOD:")))]
                    [(CHOWN)
                     (let* ([owner+path (get-params params)]
                            [owner (regexp-match #rx"[^ ]+" owner+path)])
                       (if owner
                           (let ([path (get-params owner+path)])
                             (if path
                                 (chown (car owner) (build-ftp-spath* path))
                                 (print-crlf/encoding** 'SYNTAX-ERROR "CHOWN:")))
                           (print-crlf/encoding** 'SYNTAX-ERROR "CHOWN:")))]
                    [(CHGRP)
                     (let* ([group+path (get-params params)]
                            [group (regexp-match #rx"[^ ]+" group+path)])
                       (if group
                           (let ([path (get-params group+path)])
                             (if path
                                 (chgrp (car group) (build-ftp-spath* path))
                                 (print-crlf/encoding** 'SYNTAX-ERROR "CHGRP:")))
                           (print-crlf/encoding** 'SYNTAX-ERROR "CHGRP:")))]
                    (else (print-crlf/encoding** 'MISSING-PARAMS))))
                (print-crlf/encoding** 'PERM-DENIED))
            (print-crlf/encoding** 'SYNTAX-ERROR ""))))
    
    (define-syntax (passive-host stx)
      #'(passive-host&ports-host pasv-host&ports))
    
    (define-syntax (passive-ports-from stx)
      #'(passive-host&ports-from pasv-host&ports))
    
    (define-syntax (passive-ports-to stx)
      #'(passive-host&ports-to pasv-host&ports))
    
    (define-syntax (kill-current-ftp-process stx)
      #'(custodian-shutdown-all current-process))
    
    (define-syntax-rule (print-crlf/encoding* txt)
      (print-crlf/encoding *locale-encoding* txt *client-output-port*))
    
    (define-syntax-rule (print-crlf/encoding** tag ...)
      (print*-crlf/encoding *locale-encoding* *client-output-port* *current-lang* server-responses tag ...))
    
    (define-syntax-rule (print-log-event msg ...)
      (short-print-log-event log-output-port *client-host* *login* msg ...))
    
    (define-syntax-rule (build-ftp-spath* ftp-spath ...)
      (build-ftp-spath *current-dir* ftp-spath ...))
    
    (define-syntax-rule (ftp-access-ok? ftp-spath mode)
      (ftp-access* *userstruct* (string-append *root-dir* ftp-spath) mode))
    
    (define-syntax-rule (ftp-dir-execute-ok? ftp-spath)
      (and (directory-exists? (string-append *root-dir* ftp-spath))
           (ftp-access-ok? ftp-spath 1)))
    
    (define-syntax-rule (ftp-dir-read-ok? ftp-spath)
      (and (directory-exists? (string-append *root-dir* ftp-spath))
           (ftp-access-ok? ftp-spath 5)))
    
    (define-syntax-rule (ftp-dir-write-ok? ftp-spath)
      (and (directory-exists? (string-append *root-dir* ftp-spath))
           (ftp-access-ok? ftp-spath 3)))
    
    (define-syntax-rule (ftp-dir-list-ok? ftp-spath)
      (and current-user-list-ok? (ftp-dir-read-ok? ftp-spath)))
    
    (define-syntax (current-user-list-ok? stx)
      #'(when (ftp-user-ftp-perm *userstruct*)
          (ftp-permissions-l? (ftp-user-ftp-perm *userstruct*))))
    
    (define-syntax (current-user-mkdir-ok? stx)
      #'(when (ftp-user-ftp-perm *userstruct*)
          (ftp-permissions-m? (ftp-user-ftp-perm *userstruct*))))
    
    (define-syntax (current-user-store&append-ok? stx)
      #'(when (ftp-user-ftp-perm *userstruct*)
          (ftp-permissions-c? (ftp-user-ftp-perm *userstruct*))))
    
    (define-syntax (current-user-delete-ok? stx)
      #'(when (ftp-user-ftp-perm *userstruct*)
          (ftp-permissions-d? (ftp-user-ftp-perm *userstruct*))))
    
    (define-syntax (current-user-rename-ok? stx)
      #'(when (ftp-user-ftp-perm *userstruct*)
          (ftp-permissions-f? (ftp-user-ftp-perm *userstruct*))))
    
    (define-syntax-rule (ftp-dir-store-ok? ftp-spath)
      (and current-user-store&append-ok? (ftp-dir-write-ok? ftp-spath)))
    
    (define-syntax-rule (deltail/ spath)
      (if (eq? (string-ref spath (sub1 (string-length spath))) #\/)
          (substring spath 0 (sub1 (string-length spath)))
          spath))
    
    (define-syntax-rule (ftp-obj-exists? ftp-spath)
      (let* ([stat (ftp-stat (string-append *root-dir* ftp-spath))]
             [mode (and stat (Stat-mode stat))])
        (and stat
             (or (bitwise-bit-set? mode 15)
                 (when (and (bitwise-bit-set? mode 14)
                            (not (bitwise-bit-set? mode 13)))
                   (ftp-access-ok? ftp-spath 1))))))
    
    (define-syntax-rule (ftp-obj-stat ftp-spath)
      (let* ([stat (ftp-stat (string-append *root-dir* ftp-spath))]
             [mode (and stat (Stat-mode stat))])
        (and stat
             (or (bitwise-bit-set? mode 15)
                 (when (and (bitwise-bit-set? mode 14)
                            (not (bitwise-bit-set? mode 13)))
                   (ftp-access-ok? ftp-spath 1)))
             stat)))
    
    (define-syntax-rule (ftp-file-type-obj-exists? ftp-spath)
      (let ([stat (ftp-stat (string-append *root-dir* ftp-spath))])
        (and stat (bitwise-bit-set? (Stat-mode stat) 15))))
    
    (define (mdtm-time-format->seconds mdtm)
      (and (= (string-length mdtm) 14)
           (regexp-match? #rx"^[0-9]+$" mdtm)
           (let ([year (string->number (substring mdtm 0 4))]
                 [month (string->number (substring mdtm 4 6))]
                 [day (string->number (substring mdtm 6 8))]
                 [hour (string->number (substring mdtm 8 10))]
                 [minute (string->number (substring mdtm 10 12))]
                 [second (string->number (substring mdtm 12 14))])
             (and (>= year 1970) (<= year 2037)
                  (>= month 1) (<= month 12)
                  (>= day 1) (<= day 31)
                  (when (= year 1970) (>= hour 3))
                  (<= hour 23)
                  (<= minute 59)
                  (<= second 59)
                  (find-seconds second minute hour day month year #f)))))
    
    (define (seconds->mdtm-time-format seconds)
      (let ([dte (seconds->date seconds #f)])
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
    
    (define (mlst-info ftp-path [full-path? #t][out-name #f])
      (let* ([ftp-path (simplify-ftp-path ftp-path)]
             [path (string-append *root-dir* ftp-path)]
             [parent-path (and (not (string=? "/" ftp-path))
                               (string-append *root-dir* (simplify-ftp-path ftp-path 1)))]
             [parent-stat (and parent-path (ftp-stat parent-path))]
             [name (if (string=? "/" ftp-path) "/" (path->string (file-name-from-path ftp-path)))]
             [stat (ftp-lstat path)]
             [mode (Stat-mode stat)]
             [owner (uid->uname (Stat-uid stat))]
             [group (gid->gname (Stat-gid stat))]
             [file? (bitwise-bit-set? mode 15)]
             [parent-mode (and parent-stat (Stat-mode parent-stat))]
             [user *userstruct*]
             [uid (ftp-user-uid user)]
             [gid (ftp-user-gid user)]
             [features *mlst-features*])
        (string-append (if (ftp-mlst-features-type? features)
                           (format "Type=~a;"
                                   (cond
                                     [(bitwise-bit-set? mode 15)
                                      (cond
                                        [(bitwise-bit-set? mode 13) "OS.unix=slink"]
                                        [(bitwise-bit-set? mode 14) "OS.unix=socket"]
                                        [else "file"])]
                                     [(bitwise-bit-set? mode 14)
                                      (if (bitwise-bit-set? mode 13)
                                          "OS.unix=blkdev"
                                          (if out-name
                                              (cond 
                                                [(string=? out-name ".") "cdir"]
                                                [(string=? out-name "..") "pdir"]
                                                [else "dir"])
                                              "dir"))]
                                     [(bitwise-bit-set? mode 13) "OS.unix=chrdev"]
                                     [(bitwise-bit-set? mode 12) "OS.unix=pipe"]
                                     [else "file"]))
                           "")
                       (if (and file? (ftp-mlst-features-size? features))
                           (format "Size=~d;" (Stat-size stat))
                           "")
                       (if (ftp-mlst-features-modify? features)
                           (format "Modify=~a;" (seconds->mdtm-time-format (Stat-mtime stat)))
                           "")
                       (if (ftp-mlst-features-perm? features)
                           (format "Perm=~a;"
                                   (if (grpmember? root-gid uid)
                                       (if file? "rawfd" "elcmfd")
                                       (let ([i (cond
                                                  ((= (Stat-uid stat) uid) 8)
                                                  ((= (Stat-gid stat) gid) 5)
                                                  (else 2))]
                                             [p (and parent-stat
                                                     (cond
                                                       ((= (Stat-uid parent-stat) uid) 8)
                                                       ((= (Stat-gid parent-stat) gid) 5)
                                                       (else 2)))])
                                         (string-append
                                          (if (and (not file?) 
                                                   (bitwise-bit-set? mode (- i 2)))
                                              "e" "")
                                          (if file?
                                              (if (and (if (ftp-user-ftp-perm user)
                                                           (ftp-permissions-r? (ftp-user-ftp-perm user))
                                                           #t)
                                                       (bitwise-bit-set? mode i))
                                                  "r" "")
                                              (if (and (if (ftp-user-ftp-perm user)
                                                           (ftp-permissions-l? (ftp-user-ftp-perm user))
                                                           #t)
                                                       (bitwise-bit-set? mode i))
                                                  "l" ""))
                                          (if file?
                                              (if (and (if (ftp-user-ftp-perm user)
                                                           (ftp-permissions-a? (ftp-user-ftp-perm user))
                                                           #t)
                                                       (bitwise-bit-set? mode (sub1 i)))
                                                  "a" "")
                                              (if (and (if (ftp-user-ftp-perm user)
                                                           (ftp-permissions-c? (ftp-user-ftp-perm user))
                                                           #t)
                                                       (bitwise-bit-set? mode (sub1 i)))
                                                  "c" ""))
                                          (if (and parent-stat
                                                   (bitwise-bit-set? parent-mode (sub1 p)))
                                              (string-append
                                               (if (and file? 
                                                        (if (ftp-user-ftp-perm user)
                                                            (ftp-permissions-c? (ftp-user-ftp-perm user))
                                                            #t))
                                                   "w" "")
                                               (if (or (not (ftp-user-ftp-perm user))
                                                       (ftp-permissions-f? (ftp-user-ftp-perm user)))
                                                   "f" "")
                                               (if (or (not (ftp-user-ftp-perm user))
                                                       (ftp-permissions-d? (ftp-user-ftp-perm user)))
                                                   "d" ""))
                                              "")))))
                           "")
                       (if (ftp-mlst-features-unique? features)
                           (format "Unique=~x+~x;" (Stat-dev stat) (Stat-ino stat))
                           "")
                       (if (ftp-mlst-features-charset? features)
                           (format "Charset=~a;" *locale-encoding*)
                           "")
                       (if (ftp-mlst-features-unix-mode? features)
                           (format "UNIX.mode=~a;" 
                                   (let ([perm (number->string (bitwise-and (Stat-mode stat) #o7777) 8)])
                                     (string-append (make-string (- 4 (string-length perm)) #\0) perm)))
                           "")
                       (if (ftp-mlst-features-unix-owner? features)
                           (format "UNIX.owner=~a;" (or owner (Stat-uid stat)))
                           "")
                       (if (ftp-mlst-features-unix-group? features)
                           (format "UNIX.group=~a;" (or group (Stat-gid stat)))
                           "")
                       " "
                       (or out-name (if full-path? ftp-path name)))))
    
    (define (init)
      (set! *cmd-list*
            (remove*
             (map (compose string-upcase symbol->string) disable-commands)
             `(("ABOR" #f ,ABOR-COMMAND . "ABOR")
               ("ALLO" #f ,ALLO-COMMAND . "ALLO <SP> <decimal-integer>")
               ("APPE" #f ,APPE-COMMAND . "APPE <SP> <pathname>")
               ("CDUP" #f ,CDUP-COMMAND . "CDUP")
               ("CLNT" #f ,CLNT-COMMAND . "CLNT <SP> <client-name>")
               ("CWD"  #f ,CWD-COMMAND . "CWD <SP> <pathname>")
               ("DELE" #f ,DELE-COMMAND . "DELE <SP> <pathname>")
               ("EPRT" #f ,EPRT-COMMAND . "EPRT <SP> <d> <address-family> <d> <ip-addr> <d> <port> <d>")
               ("EPSV" #f ,EPSV-COMMAND . "EPSV [<SP> (<address-family> | ALL)]")
               ("FEAT" #t ,FEAT-COMMAND . "FEAT")
               ("HELP" #t ,HELP-COMMAND . "HELP [<SP> <string>]")
               ("LANG" #t ,LANG-COMMAND . "LANG <SP> <lang-tag>")
               ("LIST" #f ,(λ (params) (DIR-LIST params)) . "LIST [<SP> <pathname>]")
               ("MDTM" #f ,MDTM-COMMAND . "MDTM <SP> <pathname>")
               ("MFF"  #f ,MFF-COMMAND . "MFF <SP> (<mff-fact> = <value> ;)+ <SP> <pathname>")
               ("MFMT" #f ,MFMT-COMMAND . "MFMT <SP> time <SP> <pathname>")
               ("MKD"  #f ,MKD-COMMAND . "MKD <SP> <pathname>")
               ("MLSD" #f ,MLSD-COMMAND . "MLSD [<SP> <pathname>]")
               ("MLST" #f ,MLST-COMMAND . "MLST [<SP> <pathname>]")
               ("MODE" #f ,MODE-COMMAND . "MODE <SP> <mode-code>")
               ("NLST" #f ,(λ (params) (DIR-LIST params #t)) . "NLST [<SP> <pathname>]")
               ("NOOP" #t ,NOOP-COMMAND . "NOOP")
               ("OPTS" #f ,OPTS-COMMAND . "OPTS <SP> <command-name> [<SP> <command-options>]")
               ("PASS" #t ,PASS-COMMAND . "PASS <SP> <password>")
               ("PASV" #f ,PASV-COMMAND . "PASV")
               ("PBSZ" #f ,PBSZ-COMMAND . "PBSZ <SP> <num>") ; error!
               ("PORT" #f ,PORT-COMMAND . "PORT <SP> <host-port>")
               ("PROT" #f ,PROT-COMMAND . "PROT <SP> <code>") ; error!
               ("PWD"  #f ,PWD-COMMAND . "PWD")
               ("QUIT" #t ,QUIT-COMMAND . "QUIT")
               ("REIN" #f ,REIN-COMMAND . "REIN")
               ("REST" #f ,REST-COMMAND . "REST <SP> <marker>")
               ("RETR" #f ,RETR-COMMAND . "RETR <SP> <pathname>")
               ("RMD"  #f ,RMD-COMMAND . "RMD <SP> <pathname>")
               ("RNFR" #f ,RNFR-COMMAND . "RNFR <SP> <pathname>")
               ("RNTO" #f ,RNTO-COMMAND . "RNTO <SP> <pathname>")
               ("SITE" #f ,SITE-COMMAND . "SITE <SP> <string>")
               ("SIZE" #f ,SIZE-COMMAND . "SIZE <SP> <pathname>")
               ("STAT" #f ,STAT-COMMAND . "STAT [<SP> <pathname>]")
               ("STOR" #f ,STOR-COMMAND . "STOR <SP> <pathname>")
               ("STOU" #f ,STOU-COMMAND . "STOU")
               ("STRU" #f ,STRU-COMMAND . "STRU <SP> <structure-code>")
               ("SYST" #t ,SYST-COMMAND . "SYST")
               ("TYPE" #f ,TYPE-COMMAND . "TYPE <SP> <type-code>")
               ("USER" #t ,USER-COMMAND . "USER <SP> <username>")
               ("XCUP" #f ,CDUP-COMMAND . "XCUP")
               ("XCWD" #f ,CWD-COMMAND . "XCWD <SP> <pathname>")
               ("XMKD" #f ,MKD-COMMAND . "XMKD <SP> <pathname>")
               ("XPWD" #f ,PWD-COMMAND . "XPWD")
               ("XRMD" #f ,RMD-COMMAND . "XRMD <SP> <pathname>"))
             (λ (a b) (string-ci=? a (car b)))))
      (set! *cmd-voc* (make-hash *cmd-list*)))
    
    (init)))

(define host/c (or/c host-string? not))
(define ssl-protocol/c (or/c ssl-protocol? not))
(define not-null-string/c (and/c string? (λ(str) (not (string=? str "")))))
(define path/c (or/c path-string? not))
(define ftp-perm/c (or/c symbol? not))

(define/contract ftp-server%
  (class/c (init-field [welcome-message         not-null-string/c]
                       
                       [server-host             host/c]
                       [server-port             port-number?]
                       [ssl-protocol            ssl-protocol/c]
                       [ssl-key                 path/c]
                       [ssl-certificate         path/c]
                       
                       [max-allow-wait          exact-positive-integer?]
                       [transfer-wait-time      exact-positive-integer?]
                       [max-clients-per-IP      exact-positive-integer?]
                       
                       [bad-auth-sleep-sec      exact-nonnegative-integer?]
                       [max-auth-attempts       exact-positive-integer?]
                       [pass-sleep-sec          exact-nonnegative-integer?]
                       
                       [disable-ftp-commands    (listof symbol?)]
                       
                       [pasv-host&ports         passive-host&ports?]
                       [allow-foreign-address   boolean?]
                       
                       [default-root-dir        path-string?]
                       [default-locale-encoding string?]
                       [log-file                path/c])
           [useradd (not-null-string/c not-null-string/c boolean? ftp-perm/c path/c . ->m . void?)])
  
  (class (ftp-utils% object%)
    (super-new)
    ;;
    ;; ---------- Public Definitions ----------
    ;;
    (init-field [welcome-message         "Racket FTP Server!"]
                
                [server-host             "127.0.0.1"]
                [server-port             21]
                [ssl-protocol            #f]
                [ssl-key                 #f]
                [ssl-certificate         #f]
                
                [max-allow-wait          25]
                [transfer-wait-time      120]
                [max-clients-per-IP      5]
                
                [bad-auth-sleep-sec      60]
                [max-auth-attempts       5]
                [pass-sleep-sec          0]
                
                [disable-ftp-commands    null]
                [allow-foreign-address   #f]
                
                [pasv-host&ports         (make-passive-host&ports "127.0.0.1" 40000 40999)]
                
                [default-root-dir        "ftp-dir"]
                [default-locale-encoding "UTF-8"]
                [log-file                #f])
    ;;
    ;; ---------- Private Definitions ----------
    ;;
    (define users-table (make-users-table))
    (define clients-table (make-hash))
    (define state 'stopped)
    (define server-custodian #f)
    (define server-thread #f)
    (define random-gen (make-pseudo-random-generator))
    ;;
    ;; ---------- Public Methods ----------
    ;;
    (define/public (useradd login real-user anonymous? [ftp-perm #f] [root-dir "/"])
      (ftp-useradd users-table login real-user anonymous? ftp-perm root-dir))
    
    (define/public (clear-users-table)
      (clear-users-info users-table))
    
    (define/public (start)
      (when (eq? state 'stopped)
        (set! server-custodian (make-custodian))
        (parameterize ([current-custodian server-custodian])
          (unless (directory-exists? default-root-dir)
            (ftp-mkdir default-root-dir))
          (when (and server-host server-port)
            (let* ([ssl-server-ctx
                    (and/exc ssl-protocol ssl-key ssl-certificate
                             (let ([ctx (ssl-make-server-context ssl-protocol)])
                               (ssl-load-certificate-chain! ctx ssl-certificate default-locale-encoding)
                               (ssl-load-private-key! ctx ssl-key #t #f default-locale-encoding)
                               ctx))]
                   [ssl-client-ctx
                    (and/exc ssl-protocol ssl-key ssl-certificate
                             (let ([ctx (ssl-make-client-context ssl-protocol)])
                               (ssl-load-certificate-chain! ctx ssl-certificate default-locale-encoding)
                               (ssl-load-private-key! ctx ssl-key #t #f default-locale-encoding)
                               ctx))]
                   [tcp-listener (tcp-listen server-port max-allow-wait #t server-host)])
              (letrec ([main-loop (λ ()
                                    (send (new ftp-session%
                                               [welcome-message welcome-message]
                                               [server-host server-host]
                                               [pasv-host&ports pasv-host&ports]
                                               [ssl-server-context ssl-server-ctx]
                                               [ssl-client-context ssl-client-ctx]
                                               [users-table users-table]
                                               [clients-table clients-table]
                                               [random-gen random-gen]
                                               [server-responses default-server-responses]
                                               [default-locale-encoding default-locale-encoding]
                                               [default-root-dir (if (path? default-root-dir)
                                                                     (path->string default-root-dir)
                                                                     default-root-dir)]
                                               [bad-auth-sleep-sec bad-auth-sleep-sec]
                                               [max-auth-attempts max-auth-attempts]
                                               [bad-auth-table (make-hash)]
                                               [pass-sleep-sec pass-sleep-sec]
                                               [allow-foreign-address allow-foreign-address]
                                               [log-output-port (if log-file
                                                                    (begin
                                                                      (unless (file-exists? log-file)
                                                                        (make-directory* (path-only log-file)))
                                                                      (open-output-file log-file #:exists 'append))
                                                                    (current-output-port))]
                                               [disable-commands disable-ftp-commands])
                                          handle-client-request 
                                          tcp-listener transfer-wait-time max-clients-per-IP)
                                    (main-loop))])
                (set! server-thread (thread main-loop))))))
        (set! state 'running)))
    
    (define/public (stop)
      (when (eq? state 'running)
        (custodian-shutdown-all server-custodian)
        (set! state 'stopped)))
    
    (define/public (status) state)))
