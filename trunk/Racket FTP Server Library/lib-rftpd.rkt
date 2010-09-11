#| 11.09.2010 14:47

Racket FTP Server Library v1.0.9
----------------------------------------------------------------------

Summary:
This file is part of Racket FTP Server.

License:
Copyright (c) 2010 Mikhail Mosienko <cnet@land.ru>
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
         srfi/48)

(provide ftp-server
         add-ftp-user
         set-ftp-root-dir
         set-default-locale-encoding
         set-log-output-port
         set-passive-ports)

(struct ftp-user (full-name login pass group home-dirs root-dir))
(struct ftp-active-host-port (host port))
(struct ftp-passive-host-port (host port) #:mutable)
(struct ftp-mlst-features (size? modify? perm?) #:mutable)
(struct ftp-session (user-id
                     user-logged
                     client-host
                     root-dir
                     current-dir
                     transfer-mode
                     representation-type
                     active-host-port
                     passive-host-port
                     current-process
                     rename-path
                     locale-encoding
                     mlst-features) #:mutable)
;;
;; ---------- Global Definitions ----------
;;
(define ftp-default-locale-encoding "UTF-8")

(define ftp-run-date (srfi/19:current-date))
(define ftp-date-zone-offset (srfi/19:date-zone-offset ftp-run-date))

(define ftp-root-dir "ftp-dir")
(define ftp-users (make-hash))

(define dead-process (make-custodian))

(define passive-ports-from 40000)
(define passive-ports-to 40599)
(define current-passive-port passive-ports-from)
(define passive-listeners #f)

(define ftp-log-output-port (current-output-port))
;;
;; ---------- Main ----------
;;
(define (ftp-server [port 21] [max-allow-wait 50] [host "127.0.0.1"])
  (let ([server-custodian (make-custodian)])
    (init-ftp-dirs ftp-root-dir)
    (init-passive-listeners server-custodian host)
    (parameterize ([current-custodian server-custodian])
      (letrec ([listener (tcp-listen port max-allow-wait #t host)]
               [main-loop (λ ()
                            (handle-client-request listener)
                            (main-loop))])
        (thread main-loop)))
    server-custodian))

(define (handle-client-request listener)
  (let ([cust (make-custodian)])
    (parameterize ([current-custodian cust])
      (let-values ([(in out) (tcp-accept listener)])
        (thread (λ ()
                  (let-values ([(server-host client-host) (tcp-addresses in)])
                    ;(fprintf ftp-log-output-port "[~a] Accept connection!\n" client-host)
                    (accept-client-request in out (connect-shutdown 60 cust client-host) client-host)
                    ;(fprintf ftp-log-output-port "[~a] Connection close!\n" client-host)
                    )
                  (custodian-shutdown-all cust)))))))

(define (connect-shutdown time connect-cust client-host)
  (parameterize ([current-custodian connect-cust])
    (let* ([tick time]
           [reset (λ () (set! tick time))])
      (thread (λ ()
                (let loop ()
                  (when (> tick 0)
                    (sleep 1)
                    (set! tick (sub1 tick))
                    (loop)))
                ;(fprintf ftp-log-output-port "[~a] Auto connection close!\n" client-host)
                (custodian-shutdown-all connect-cust)))
      reset)))

(define (accept-client-request input-port output-port reset-timer client-host)
  (with-handlers ([any/c #|displayln|# void])
    (eval-client-request input-port output-port reset-timer client-host))
  (flush-output output-port))

(define (eval-client-request input-port output-port reset-timer client-host)
  (let ([session (ftp-session #f           ; user-id
                              #f           ; user-logged
                              client-host  ; client-host
                              ftp-root-dir ; root-dir
                              "/"          ; current-dir
                              'active      ; transfer-mode
                              'ASCII       ; representation-type
                              ; active-data-host-port
                              (ftp-active-host-port (bytes 127 0 0 1) 20)
                              ; passive-host-port
                              (ftp-passive-host-port (bytes 127 0 0 1) 20)
                              dead-process                ; current-process
                              #f                          ; rename-path
                              ftp-default-locale-encoding ; locale-encoding
                              ; mlst-features
                              (ftp-mlst-features #t #t #f))])
    (print-crlf/encoding* session output-port "220 Racket FTP Server!")
    (flush-output output-port)
    ;(sleep 1)
    (let loop ([request (read-request session input-port)])
      (unless (eof-object? request)
        (when request
          ;(printf "[~a] ~a\n" client-host request)
          (let ([cmd (string->symbol (string-upcase (car (regexp-match #rx"[^ ]+" request))))]
                [params (get-params request)])
            (case cmd
              ((USER)
               (USER-COMMAND params session output-port))
              ((PASS)
               (PASS-COMMAND params session output-port))
              ((QUIT)
               (print-crlf/encoding* session output-port "221 Goodbye.")
               (flush-output output-port)
               (raise 0))
              (else
               (if (ftp-session-user-logged session)
                   (case cmd
                     ((PWD XPWD)
                      (print-crlf/encoding* session output-port
                                            (format "257 ~s is current directory." (ftp-session-current-dir session))))
                     ((PORT)
                      (PORT-COMMAND params session output-port))
                     ((CWD)
                      (CWD-COMMAND params session output-port))
                     ((CDUP)
                      (set-ftp-session-current-dir! session
                                                    (simplify-ftp-path (ftp-session-current-dir session) 1))
                      (print-crlf/encoding* session output-port "250 CDUP command successful."))
                     ((NLST)
                      (DIR-LIST #t params session output-port))
                     ((LIST)
                      (DIR-LIST #f params session output-port))
                     ((MLST)
                      (MLST-COMMAND params session output-port))
                     ((MLSD)
                      (MLSD-COMMAND params session output-port))
                     ((TYPE)
                      (TYPE-COMMAND params session output-port))
                     ((ABOR)
                      (kill-current-ftp-process session)
                      (print-crlf/encoding* session output-port "226 Abort successful."))
                     ((PASV)
                      (PASV-COMMAND params session output-port))
                     ((RETR)
                      (RETR-COMMAND params session output-port))
                     ((MKD)
                      (MKD-COMMAND params session output-port))
                     ((RMD)
                      (RMD-COMMAND params session output-port))
                     ((STOR)
                      (STORE-FILE params session output-port))
                     ((APPE)
                      (STORE-FILE params session output-port 'append))
                     ((DELE)
                      (DELE-COMMAND params session output-port))
                     ((RNFR)
                      (RNFR-COMMAND params session output-port))
                     ((RNTO)
                      (RNTO-COMMAND params session output-port))
                     ((STOU)
                      (STOU-FILE params session output-port))
                     ((MDTM)
                      (MDTM-COMMAND params session output-port))
                     ((SIZE)
                      (SIZE-COMMAND params session output-port))
                     ((NOOP)
                      (print-crlf/encoding* session output-port "200 NOOP"))
                     ((SITE)
                      (SITE-COMMAND params session output-port))
                     ((SYST)
                      (print-crlf/encoding* session output-port "215 UNIX (Unix-like)"))
                     ((OPTS)
                      (OPTS-COMMAND params session output-port))
                     ((FEAT)
                      (print-crlf/encoding* session output-port "211-Extensions supported:")
                      (print-crlf/encoding* session output-port " UTF8")
                      (print-crlf/encoding* session output-port " MLST size*;modify*;perm")
                      (print-crlf/encoding* session output-port " MLSD")
                      (print-crlf/encoding* session output-port " SIZE")
                      (print-crlf/encoding* session output-port " MDTM")
                      (print-crlf/encoding* session output-port "211 End"))
                     ((MODE)
                      (MODE-COMMAND params session output-port))
                     ((STRU)
                      (STRU-COMMAND params session output-port))
                     (else (print-crlf/encoding* session output-port
                                                 (format "502 ~a not implemented." (symbol->string cmd)))))
                   (print-crlf/encoding* session  output-port "530 Please login with USER and PASS.")))))
          (flush-output output-port))
        (reset-timer)
        (sleep .005)
        (loop (read-request session input-port))))))

;; Возвращает идентификатор(имя) пользователя
(define (USER-COMMAND params session output-port)
  (if params
      (let ([name params])
        (if (and (hash-ref ftp-users name #f)
                 (string=? (ftp-user-pass (hash-ref ftp-users name)) ""))
            (print-crlf/encoding* session output-port
                                  "331 Anonymous login ok, send your complete email address as your password.")
            (print-crlf/encoding* session output-port (format "331 Password required for ~a" name)))
        (set-ftp-session-user-id! session name))
      (begin
        (print-crlf/encoding* session output-port "501 Syntax error in parameters or arguments.")
        (set-ftp-session-user-id! session #f)))
  (set-ftp-session-user-logged! session #f))

;; Проверяет пороль пользователя
(define (PASS-COMMAND params session output-port)
  (let ([correct?
         (cond
           ((string? (ftp-session-user-id session))
            (let ([pass params])
              (cond
                ((not (hash-ref ftp-users (ftp-session-user-id session) #f))
                 (print-log-event session "Login incorrect.")
                 (print-crlf/encoding* session output-port "530 Login incorrect.")
                 #f)
                ((string=? (ftp-user-pass (hash-ref ftp-users (ftp-session-user-id session)))
                           "")
                 (print-log-event session "User logged in.")
                 (print-crlf/encoding* session output-port "230 Anonymous access granted.")
                 #t)
                ((not pass)
                 (print-log-event session "Login incorrect.")
                 (print-crlf/encoding* session output-port "530 Login incorrect.")
                 #f)
                ((string=? (ftp-user-pass (hash-ref ftp-users (ftp-session-user-id session)))
                           pass)
                 (print-log-event session "User logged in.")
                 (print-crlf/encoding* session output-port
                                       (format "230 User ~a logged in." (ftp-session-user-id session)))
                 #t)
                (else
                 (print-log-event session "Password incorrect.")
                 (print-crlf/encoding* session output-port "530 Login incorrect.")
                 #f))))
           (else
            (print-crlf/encoding* session output-port "530 Login incorrect.")
            #f))])
    (set-ftp-session-user-logged! session correct?)
    (when correct?
      (set-ftp-session-root-dir! session (ftp-user-root-dir (hash-ref ftp-users (ftp-session-user-id session)))))))

;; Инициирует активный режим.
(define (PORT-COMMAND params session output-port)
  (with-handlers ([any/c (λ (e) (print-crlf/encoding* session output-port
                                                      "501 Syntax error in parameters or arguments."))])
    (unless (regexp-match #rx"[0-9]+,[0-9]+,[0-9]+,[0-9]+,[0-9]+,[0-9]+" params)
      (raise 'error))
    (let* ([l (regexp-split #rx"," params)]
           [host (bytes (string->number (first l)) (string->number (second l))
                        (string->number (third l)) (string->number (fourth l)))]
           [port (((string->number (fifth l)). * . 256). + .(string->number (sixth l)))])
      (when (port . > . 65535) (raise 'error))
      (set-ftp-session-transfer-mode! session 'active)
      (set-ftp-session-active-host-port! session (ftp-active-host-port host port))
      (print-crlf/encoding* session output-port "200 Port command successful."))))

;; Посылает клиенту список файлов директории.
(define (DIR-LIST short? params session output-port)
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
       (let ([info (read-ftp-sys-info ftp-path (string-append spath "/.ftp-racket-directory"))])
         (string-append "d" (car info) (second info) (third info) " 2 " (fourth info) " " (fifth info))))

     (define (read-ftp-file-sys-info ftp-path spath)
       (let ([info (read-ftp-sys-info ftp-path (string-append spath ".ftp-racket-file"))])
         (string-append "-" (car info) (second info) (third info) " 1 " (fourth info) " " (fifth info))))

     (define (dlst ftp-dir-name)
       (let* ([full-dir-name (string-append (ftp-session-root-dir session) ftp-dir-name)]
              [reptype (ftp-session-representation-type session)]
              [dirlist (string-append*
                        (map (λ (p)
                               (let* ([spath (path->string p)]
                                      [ftp-full-spath (string-append (simplify-ftp-path ftp-dir-name) "/" spath)]
                                      [full-spath (string-append full-dir-name "/" spath)])
                                 (if (or (and (ftp-file-exists? full-spath)
                                              (ftp-file-allow-read? full-spath (current-ftp-user session)))
                                         (and (ftp-dir-exists? full-spath)
                                              (ftp-dir-allow-read? full-spath (current-ftp-user session))))
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
                             (directory-list full-dir-name)))])
         (ftp-data-transfer session
                            (case reptype
                              ((ASCII) dirlist)
                              ((Image) (list-string->bytes/encoding (ftp-session-locale-encoding session) dirlist)))
                            output-port)))]

    (let ([dir (if (and params (regexp-match #rx"[^-A-z]+.*" params))
                   (regexp-match #rx"[^ ]+.*" (car (regexp-match #rx"[^-A-z]+.*" params)))
                   #f)])
      (cond
        ((not dir)
         (dlst (ftp-session-current-dir session)))
        ((memq (string-ref dir 0) '(#\/ #\\))
         (if (ftp-dir-exists? (string-append (ftp-session-root-dir session) dir))
             (dlst dir)
             (print-crlf/encoding* session output-port "550 Directory not found.")))
        ((ftp-dir-exists? (string-append (ftp-session-root-dir session) (ftp-session-current-dir session) "/" dir))
         (dlst (string-append (ftp-session-current-dir session) "/" dir)))
        (else
         (print-crlf/encoding* session output-port "550 Directory not found."))))))

;; Отправляет клиенту копию файла.
(define (RETR-COMMAND params session output-port)
  (local [(define (fcopy full-path-file)
            (ftp-data-transfer session full-path-file output-port #t))]

    (cond
      ((not params)
       (print-crlf/encoding* session output-port "501 Syntax error in parameters or arguments."))
      ((and (memq (string-ref params 0) '(#\/ #\\))
            (ftp-file-exists? (string-append (ftp-session-root-dir session) params)))
       (if (ftp-file-allow-read? (string-append (ftp-session-root-dir session) params)
                                 (current-ftp-user session))
           (fcopy (string-append (ftp-session-root-dir session) params))
           (print-crlf/encoding* session output-port "550 Permission denied.")))
      ((ftp-file-exists? (string-append (ftp-session-root-dir session) (ftp-session-current-dir session) "/" params))
       (if (ftp-file-allow-read? (string-append (ftp-session-root-dir session)
                                                (ftp-session-current-dir session) "/" params)
                                 (current-ftp-user session))
           (fcopy (string-append (ftp-session-root-dir session) (ftp-session-current-dir session) "/" params))
           (print-crlf/encoding* session output-port "550 Permission denied.")))
      (else
       (print-crlf/encoding* session output-port "550 Directory not found.")))))

;; Устанавливает тип передачи файла (реализован только A(текстовый) и I(бинарный)).
(define (TYPE-COMMAND params session output-port)
  (if params
      (case (string->symbol (string-upcase (car (regexp-split #rx" +" params))))
        ((A)
         (set-ftp-session-representation-type! session 'ASCII)
         (print-crlf/encoding* session output-port "200 Type set to A."))
        ((I)
         (set-ftp-session-representation-type! session 'Image)
         (print-crlf/encoding* session output-port "200 Type set to I."))
        ((E L)
         (print-crlf/encoding* session output-port "504 Command not implemented for that parameter."))
        (else
         (print-crlf/encoding* session output-port "501 Unsupported type. Supported types are I and A.")))
      (print-crlf/encoding* session output-port "501 Syntax error in parameters or arguments.")))

;; Experimental
(define (MODE-COMMAND params session output-port)
  (if params
      (case (string->symbol (string-upcase params))
        ((S)
         (print-crlf/encoding* session output-port "200 Mode set to S."))
        ((B C)
         (print-crlf/encoding* session output-port "504 Command not implemented for that parameter."))
        (else
         (print-crlf/encoding* session output-port "501 Unknown MODE type.")))
      (print-crlf/encoding* session output-port "501 Syntax error in parameters or arguments.")))

;; Experimental
(define (STRU-COMMAND params session output-port)
  (if params
      (case (string->symbol (string-upcase params))
        ((F)
         (print-crlf/encoding* session output-port "200 FILE STRUCTURE set to F (no record structure)."))
        ((R P)
         (print-crlf/encoding* session output-port "504 Command not implemented for that parameter."))
        (else
         (print-crlf/encoding* session output-port "501 Unknown FILE STRUCTURE type.")))
      (print-crlf/encoding* session output-port "501 Syntax error in parameters or arguments.")))

;; Необходима для смены рабочей директории (current-dir).
(define (CWD-COMMAND params session output-port)
  (cond
    ((not params)
     (print-crlf/encoding* session output-port "501 Syntax error in parameters or arguments."))
    ((and (memq (string-ref params 0) '(#\/ #\\))
          (ftp-dir-exists? (string-append (ftp-session-root-dir session) params)))
     (set-ftp-session-current-dir! session (simplify-ftp-path params))
     (print-crlf/encoding* session output-port "250 CWD command successful."))
    ((ftp-dir-exists? (string-append (ftp-session-root-dir session) (ftp-session-current-dir session) "/" params))
     (set-ftp-session-current-dir! session
                                   (simplify-ftp-path (string-append (ftp-session-current-dir session)
                                                                     "/" params)))
     (print-crlf/encoding* session output-port "250 CWD command successful."))
    (else
     (print-crlf/encoding* session output-port "550 Directory not found."))))

;; Создает каталог.
(define (MKD-COMMAND params session output-port)
  (local [(define (mkd ftp-parent-path dir-name user)
            (let* ([full-parent-path (string-append (ftp-session-root-dir session) ftp-parent-path)]
                   [sp (string-append full-parent-path "/" dir-name)])
              (if (ftp-dir-allow-write? full-parent-path user)
                  (if (ftp-dir-exists? sp)
                      (print-crlf/encoding* session output-port "550 Can't create directory. Directory exist!")
                      (let* ([fpp (simplify-ftp-path ftp-parent-path)]
                             [fp (string-append fpp (if (string=? fpp "/") "" "/") dir-name)])
                        (ftp-mkdir* sp (ftp-user-login user) (ftp-user-group user))
                        (print-log-event session (format "Make directory ~a" fp))
                        (print-crlf/encoding* session output-port
                                              (format "257 ~s - Directory successfully created." fp))))
                  (print-crlf/encoding* session output-port
                                        "550 Can't create directory. Permission denied!"))))]

    (if params
        (let* ([path (if (memq (string-ref params (sub1 (string-length params))) '(#\/ #\\))
                         (car (regexp-match #rx".*[^/\\\\]+" params))
                         params)]
               [dir-name (if (file-name-from-path path)
                             (path->string (file-name-from-path path))
                             #f)]
               [parent-path (if (and dir-name (path-only path))
                                (path->string (path-only path))
                                #f)]
               [user (current-ftp-user session)]
               [curr-dir (ftp-session-current-dir session)])
          (cond
            ((not dir-name)
             (print-crlf/encoding* session output-port "550 Can't create directory."))
            ((and (not parent-path)
                  (ftp-dir-exists? (string-append (ftp-session-root-dir session) curr-dir "/" dir-name)))
             (print-crlf/encoding* session output-port "550 Can't create directory. Directory exist!"))
            ((not parent-path)
             (mkd curr-dir dir-name user))
            ((memq (string-ref parent-path 0) '(#\/ #\\))
             (if (ftp-dir-exists? (string-append (ftp-session-root-dir session) parent-path))
                 (mkd parent-path dir-name user)
                 (print-crlf/encoding* session output-port "550 Can't create directory.")))
            ((ftp-dir-exists? (string-append (ftp-session-root-dir session) curr-dir "/" parent-path))
             (mkd (string-append curr-dir "/" parent-path) dir-name user))
            (else
             (print-crlf/encoding* session output-port "550 Can't create directory."))))
        (print-crlf/encoding* session output-port "501 Syntax error in parameters or arguments."))))

;; Удаляет каталог.
(define (RMD-COMMAND params session output-port)
  (local
    [(define (rmd ftp-path)
       (let ([spath (string-append (ftp-session-root-dir session) ftp-path)])
         (if ((file-or-directory-identity spath). = .(file-or-directory-identity (ftp-session-root-dir session)))
             (print-crlf/encoding* session output-port "550 Directory not found.")
             (if (ftp-dir-allow-delete-move? spath (current-ftp-user session))
                 (let ([lst (directory-list spath)])
                   (if (> (length lst) 1)
                       (print-crlf/encoding* session output-port
                                             "550 Can't delete directory. Directory not empty!")
                       (with-handlers ([exn:fail:filesystem? (λ (e)
                                                               (print-crlf/encoding* session output-port
                                                                                     "550 System error."))])
                         (delete-file (string-append spath "/.ftp-racket-directory"))
                         (delete-directory spath)
                         (print-log-event session (format "Remove a directory ~a" (simplify-ftp-path ftp-path)))
                         (print-crlf/encoding* session output-port "250 RMD command successful."))))
                 (print-crlf/encoding* session output-port
                                       "550 Can't delete directory. Permission denied!")))))]

    (cond
      ((and (memq (string-ref params 0) '(#\/ #\\))
            (ftp-dir-exists? (string-append (ftp-session-root-dir session) params)))
       (rmd params))
      ((ftp-dir-exists? (string-append (ftp-session-root-dir session) (ftp-session-current-dir session) "/" params))
       (rmd (string-append (ftp-session-current-dir session) "/" params)))
      (else
       (print-crlf/encoding* session output-port "550 Directory not found.")))))

;; Сохраняет файл.
(define (STORE-FILE params session output-port [exists-mode 'truncate])
  (local [(define (fstore full-path-file user)
            (ftp-store-file session full-path-file output-port exists-mode)
            (unless (file-exists? (string-append full-path-file ".ftp-racket-file"))
              (ftp-mksys-file (string-append full-path-file ".ftp-racket-file")
                              (ftp-user-login user) (ftp-user-group user))))

          (define (stor full-parent-path file-name user)
            (let ([real-path (string-append full-parent-path "/" file-name)])
              (if (and (ftp-dir-allow-write? full-parent-path user)
                       (ftp-file-name-safe? real-path)
                       (if (ftp-file-exists? real-path)
                           (ftp-file-allow-write? real-path user)
                           #t))
                  (fstore real-path user)
                  (print-crlf/encoding* session output-port "550 Can't store file. Permission denied!"))))]

    (if params
        (let* ([file-name (if (file-name-from-path params)
                              (path->string (file-name-from-path params))
                              #f)]
               [parent-path (if (and file-name (path-only params))
                                (path->string (path-only params))
                                #f)]
               [user (current-ftp-user session)]
               [curr-dir (ftp-session-current-dir session)])
          (cond
            ((not file-name)
             (print-crlf/encoding* session output-port "550 Can't store file."))
            ((not parent-path)
             (stor (string-append (ftp-session-root-dir session) curr-dir) file-name user))
            ((and (memq (string-ref parent-path 0) '(#\/ #\\))
                  (ftp-dir-exists? (string-append (ftp-session-root-dir session) parent-path)))
             (stor (string-append (ftp-session-root-dir session) parent-path) file-name))
            ((ftp-dir-exists? (string-append (ftp-session-root-dir session) curr-dir "/" parent-path))
             (stor (string-append (ftp-session-root-dir session) curr-dir "/" parent-path) file-name user))
            (else
             (print-crlf/encoding* session output-port "550 Can't store file."))))
        (print-crlf/encoding* session output-port "501 Syntax error in parameters or arguments."))))

;; Experimental
(define (STOU-FILE params session output-port)
  (let ([user (current-ftp-user session)])
    (cond
      (params
       (print-crlf/encoding* session output-port "501 Syntax error in parameters or arguments."))
      ((ftp-dir-allow-write? (string-append (ftp-session-root-dir session) (ftp-session-current-dir session)) user)
       (let* ([file-name (let loop ([fname (gensym "noname")])
                           (if (ftp-file-exists? (string-append (ftp-session-root-dir session)
                                                                (ftp-session-current-dir session)
                                                                "/" fname))
                               (loop (gensym "noname"))
                               fname))]
              [path (string-append (ftp-session-root-dir session) (ftp-session-current-dir session) "/" file-name)])
         (ftp-store-file session path output-port 'truncate)
         (ftp-mksys-file (string-append path ".ftp-racket-file")
                         (ftp-user-login user) (ftp-user-group user))))
      (else
       (print-crlf/encoding* session output-port "550 Can't store file. Permission denied!")))))

;; Удаляет файл.
(define (DELE-COMMAND params session output-port)
  (local
    [(define (dele ftp-path)
       (let ([spath (string-append (ftp-session-root-dir session) ftp-path)])
         (if (ftp-file-allow-delete-move? spath (current-ftp-user session))
             (with-handlers ([exn:fail:filesystem? (λ (e)
                                                     (print-crlf/encoding* session output-port "550 System error."))])
               (delete-file (string-append spath ".ftp-racket-file"))
               (delete-file spath)
               (print-log-event session (format "Delete a file ~a" (simplify-ftp-path ftp-path)))
               (print-crlf/encoding* session output-port "250 DELE command successful."))
             (print-crlf/encoding* session output-port
                                   "550 Can't delete file. Permission denied!"))))]

    (cond
      ((and (memq (string-ref params 0) '(#\/ #\\))
            (ftp-file-exists? (string-append (ftp-session-root-dir session) params)))
       (dele params))
      ((ftp-file-exists? (string-append (ftp-session-root-dir session) (ftp-session-current-dir session) "/" params))
       (dele (string-append (ftp-session-current-dir session) "/" params)))
      (else
       (print-crlf/encoding* session output-port "550 File not found.")))))

;; Эмулирует Unix команды.
(define (SITE-COMMAND params session output-port)
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
              [fchmod (λ (full-path target)
                        (let ([owner (vector-ref (ftp-file-or-dir-sysbytes/owner (string-append full-path target)) 1)]
                              [user (current-ftp-user session)])
                          (if (string=? owner (ftp-user-login user))
                              (begin
                                (ftp-mksys-file (string-append full-path target)
                                                (ftp-user-login user) (ftp-user-group user)
                                                (get-permis permis))
                                (print-crlf/encoding* session output-port "200 SITE CHMOD command successful."))
                              (print-crlf/encoding* session output-port "550 CHMOD: Permission denied!"))))])
         (cond
           ((memq (string-ref path 0) '(#\/ #\\))
            (cond
              ((ftp-file-exists? (string-append (ftp-session-root-dir session) path))
               (fchmod (string-append (ftp-session-root-dir session) path) ".ftp-racket-file"))
              ((ftp-dir-exists? (string-append (ftp-session-root-dir session) path))
               (fchmod (string-append (ftp-session-root-dir session) path) "/.ftp-racket-directory"))
              (else
               (print-crlf/encoding* session output-port "550 File or directory not found."))))
           ((ftp-file-exists? (string-append (ftp-session-root-dir session) (ftp-session-current-dir session) "/" path))
            (fchmod (string-append (ftp-session-root-dir session)
                                   (ftp-session-current-dir session) "/" path) ".ftp-racket-file"))
           ((ftp-dir-exists? (string-append (ftp-session-root-dir session) (ftp-session-current-dir session) "/" path))
            (fchmod (string-append (ftp-session-root-dir session)
                                   (ftp-session-current-dir session) "/" path) "/.ftp-racket-directory"))
           (else
            (print-crlf/encoding* session output-port "550 File or directory not found.")))))]

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
                         (print-crlf/encoding* session output-port "501 CHMOD: Syntax error in parameters or arguments.")))
                   (print-crlf/encoding* session output-port "501 CHMOD: Syntax error in parameters or arguments."))))
            (else (print-crlf/encoding* session output-port "504 Command not implemented for that parameter."))))
        (print-crlf/encoding* session output-port "501 Syntax error in parameters or arguments."))))

;; Experimental
(define (MDTM-COMMAND params session output-port)
  (if params
      (let ([mdtm (λ (path)
                    (if (or (ftp-file-exists? path)
                            (ftp-dir-exists? path))
                        (print-crlf/encoding* session output-port
                                              (format "213 ~a"
                                                      (seconds->mdtm-time-format (file-or-directory-modify-seconds path))))
                        (print-crlf/encoding* session output-port "550 File or directory not found.")))])
        (if (memq (string-ref params 0) '(#\/ #\\))
            (mdtm (string-append (ftp-session-root-dir session) params))
            (mdtm (string-append (ftp-session-root-dir session) (ftp-session-current-dir session) "/" params))))
      (print-crlf/encoding* session output-port "501 Syntax error in parameters or arguments.")))

;; Experimental
(define (SIZE-COMMAND params session output-port)
  (if params
      (let ([size (λ (path)
                    (if (ftp-file-exists? path)
                        (print-crlf/encoding* session output-port (format "213 ~a" (file-size path)))
                        (print-crlf/encoding* session output-port "550 File not found.")))])
        (if (memq (string-ref params 0) '(#\/ #\\))
            (size (string-append (ftp-session-root-dir session) params))
            (size (string-append (ftp-session-root-dir session) (ftp-session-current-dir session) "/" params))))
      (print-crlf/encoding* session output-port "501 Syntax error in parameters or arguments.")))

;; Experimental
(define (MLST-COMMAND params session output-port)
  (local [(define (mlst session ftp-path)
            (print-crlf/encoding* session output-port "250- Listing")
            (print-crlf/encoding* session output-port
                                  (string-append " " (mlst-info session ftp-path)))
            (print-crlf/encoding* session output-port "250 End"))]
    (cond
      ((not params)
       (let ([path (string-append (ftp-session-root-dir session) (ftp-session-current-dir session))])
         (if ((file-or-directory-identity path). = .(file-or-directory-identity (ftp-session-root-dir session)))
             (print-crlf/encoding* session output-port "550 File or directory not found.")
             (mlst session (ftp-session-current-dir session)))))
      ((memq (string-ref params 0) '(#\/ #\\))
       (let ([path (string-append (ftp-session-root-dir session) params)])
         (cond
           ((ftp-file-exists? path)
            (mlst session params))
           ((ftp-dir-exists? path)
            (if ((file-or-directory-identity path). = .(file-or-directory-identity (ftp-session-root-dir session)))
                (print-crlf/encoding* session output-port "550 File or directory not found.")
                (mlst session params)))
           (else
            (print-crlf/encoding* session output-port "550 File or directory not found.")))))
      (else
       (let ([path (string-append (ftp-session-root-dir session) (ftp-session-current-dir session) "/" params)])
         (cond
           ((ftp-file-exists? path)
            (mlst session (string-append (ftp-session-current-dir session) "/" params)))
           ((ftp-dir-exists? path)
            (if ((file-or-directory-identity path). = .(file-or-directory-identity (ftp-session-root-dir session)))
                (print-crlf/encoding* session output-port "550 File or directory not found.")
                (mlst session (string-append (ftp-session-current-dir session) "/" params))))
           (else
            (print-crlf/encoding* session output-port "550 File or directory not found."))))))))

;; Experimental
(define (MLSD-COMMAND params session output-port)
  (local [(define (mlsd ftp-path)
            (let* ([path (string-append (ftp-session-root-dir session) ftp-path)]
                   [reptype (ftp-session-representation-type session)]
                   [dirlist (string-append*
                             (map (λ (p)
                                    (let* ([spath (path->string p)]
                                           [full-spath (string-append path "/" spath)])
                                      (if (or (and (ftp-file-exists? full-spath)
                                                   (ftp-file-allow-read? full-spath (current-ftp-user session)))
                                              (and (ftp-dir-exists? full-spath)
                                                   (ftp-dir-allow-read? full-spath (current-ftp-user session))))
                                          (string-append (mlst-info session
                                                                    (string-append ftp-path "/" spath) #f)
                                                         "\n")
                                          "")))
                                  (directory-list path)))])
              (ftp-data-transfer session
                                 (case reptype
                                   ((ASCII) dirlist)
                                   ((Image) (list-string->bytes/encoding (ftp-session-locale-encoding session) dirlist)))
                                 output-port)))]
    (cond
      ((not params)
       (mlsd (ftp-session-current-dir session)))
      ((memq (string-ref params 0) '(#\/ #\\))
       (let ([path (string-append (ftp-session-root-dir session) params)])
         (if (ftp-dir-exists? path)
             (mlsd params)
             (print-crlf/encoding* session output-port "550 Directory not found."))))
      (else
       (let ([path (string-append (ftp-session-root-dir session) (ftp-session-current-dir session) "/" params)])
         (if (ftp-dir-exists? path)
             (mlsd (string-append (ftp-session-current-dir session) "/" params))
             (print-crlf/encoding* session output-port "550 Directory not found.")))))))


;; Experimental
(define (OPTS-COMMAND params session output-port)
  (if params
      (let ([cmd (string->symbol (string-upcase (car (regexp-match #rx"[^ ]+" params))))])
        (case cmd
          ((UTF8)
           (let ([mode (regexp-match #rx"[^ \t]+.+" (substring params 4))])
             (if mode
                 (case (string->symbol (string-upcase (car mode)))
                   ((ON)
                    (set-ftp-session-locale-encoding! session "UTF-8")
                    (print-crlf/encoding* session output-port "200 UTF8 mode enabled."))
                   ((OFF)
                    (set-ftp-session-locale-encoding! session ftp-default-locale-encoding)
                    (print-crlf/encoding* session output-port "200 UTF8 mode disabled."))
                   (else
                    (print-crlf/encoding* session output-port "501 UTF8: Syntax error in parameters or arguments.")))
                 (print-crlf/encoding* session output-port "501 UTF8: Syntax error in parameters or arguments."))))
          ((MLST)
           (let ([modes (regexp-match #rx"[^ \t]+.+" (substring params 4))])
             (if modes
                 (let ([mlst (map string->symbol
                                  (filter (λ (s) (not (string=? s "")))
                                          (regexp-split #rx"[; \t]+" (string-upcase (car modes)))))])
                   (if (andmap (λ (mode) (member mode '(SIZE MODIFY PERM))) mlst)
                       (let ([features (ftp-session-mlst-features session)])
                         (for-each (λ (mode)
                                     (case mode
                                       ((SIZE) (set-ftp-mlst-features-size?! features #t))
                                       ((MODIFY) (set-ftp-mlst-features-modify?! features #t))
                                       ((PERM) (set-ftp-mlst-features-perm?! features #t))))
                                   mlst)
                         (print-crlf/encoding* session output-port "200 MLST modes enabled."))
                       (print-crlf/encoding* session output-port
                                             "501 MLST: Syntax error in parameters or arguments.")))
                 (print-crlf/encoding* session output-port "501 MLST: Syntax error in parameters or arguments."))))
          (else (print-crlf/encoding* session output-port "501 Syntax error in parameters or arguments."))))
      (print-crlf/encoding* session output-port "501 Syntax error in parameters or arguments.")))

;; Подготавливает к переименованию файл или каталог.
(define (RNFR-COMMAND params session output-port)
  (if params
      (let ([path1 (string-append (ftp-session-root-dir session) params)]
            [path2 (string-append (ftp-session-root-dir session) (ftp-session-current-dir session) "/" params)])
        (cond
          ((memq (string-ref params 0) '(#\/ #\\))
           (cond
             ((ftp-file-exists? path1)
              (if (ftp-file-allow-delete-move? path1 (current-ftp-user session))
                  (begin
                    (set-ftp-session-rename-path! session path1)
                    (print-crlf/encoding* session output-port "350 File or directory exists, ready for destination name."))
                  (print-crlf/encoding* session output-port "550 Permission denied!")))
             ((ftp-dir-exists? path1)
              (if (ftp-dir-allow-delete-move? path1 (current-ftp-user session))
                  (begin
                    (set-ftp-session-rename-path! session path1)
                    (print-crlf/encoding* session output-port "350 File or directory exists, ready for destination name."))
                  (print-crlf/encoding* session output-port "550 Permission denied!")))
             (else
              (print-crlf/encoding* session output-port "550 File or directory not found."))))
          ((ftp-file-exists? path2)
           (if (ftp-file-allow-delete-move? path2 (current-ftp-user session))
               (begin
                 (set-ftp-session-rename-path! session path2)
                 (print-crlf/encoding* session output-port "350 File or directory exists, ready for destination name."))
               (print-crlf/encoding* session output-port "550 Permission denied!")))
          ((ftp-dir-exists? path2)
           (if (ftp-dir-allow-delete-move? path2 (current-ftp-user session))
               (begin
                 (set-ftp-session-rename-path! session path2)
                 (print-crlf/encoding* session output-port "350 File or directory exists, ready for destination name."))
               (print-crlf/encoding* session output-port "550 Permission denied!")))
          (else
           (print-crlf/encoding* session output-port "550 File or directory not found."))))
      (print-crlf/encoding* session output-port "501 Syntax error in parameters or arguments.")))

;; Переименовывает подготавленный файл или каталог.
(define (RNTO-COMMAND params session output-port)
  (local [(define (move file? old-path full-parent-path name user)
            (let ([new-path (string-append full-parent-path "/" name)])
              (if (ftp-dir-allow-write? full-parent-path user)
                  (if ((if file? ftp-file-exists? ftp-dir-exists?) new-path)
                      (print-crlf/encoding* session output-port "550 File or directory exist!")
                      (with-handlers ([exn:fail:filesystem?
                                       (λ (e)
                                         (print-crlf/encoding* session output-port
                                                               "550 Can't rename file or directory."))])
                        (when file?
                          (rename-file-or-directory (string-append old-path ".ftp-racket-file")
                                                    (string-append new-path ".ftp-racket-file")))
                        (rename-file-or-directory old-path new-path)
                        (print-log-event session (format "Rename the file or directory from ~a to ~a"
                                                         (real-path->ftp-path session old-path)
                                                         (real-path->ftp-path session new-path)))
                        (print-crlf/encoding* session output-port "250 Rename successful.")))
                  (print-crlf/encoding* session output-port
                                        "550 Can't rename file or directory. Permission denied!"))))]

    (if params
        (if (ftp-session-rename-path session)
            (let* ([old-path (ftp-session-rename-path session)]
                   [old-file? (ftp-file-exists? old-path)]
                   [new-dir? (memq (string-ref params (sub1 (string-length params))) '(#\/ #\\))]
                   [path (if new-dir?
                             (regexp-match #rx".*[^/\\\\]+" params)
                             params)]
                   [name (if (file-name-from-path path)
                             (path->string (file-name-from-path path))
                             #f)]
                   [parent-path (if (and name (path-only path))
                                    (path->string (path-only path))
                                    #f)]
                   [user (current-ftp-user session)]
                   [curr-dir (ftp-session-current-dir session)])
              (cond
                ((or (not name)
                     (and old-file? new-dir?))
                 (print-crlf/encoding* session output-port "550 Can't rename file or directory."))
                ((and (not parent-path)
                      ((if old-file? ftp-file-exists? ftp-dir-exists?)
                       (string-append (ftp-session-root-dir session) curr-dir "/" name)))
                 (print-crlf/encoding* session output-port "550 File or directory exist!"))
                ((not parent-path)
                 (move old-file? old-path (string-append (ftp-session-root-dir session) curr-dir) name user))
                ((and (memq (string-ref parent-path 0) '(#\/ #\\))
                      (ftp-dir-exists? (string-append (ftp-session-root-dir session) parent-path)))
                 (move old-file? old-path (string-append (ftp-session-root-dir session) parent-path) name user))
                ((ftp-dir-exists? (string-append (ftp-session-root-dir session) curr-dir "/" parent-path))
                 (move old-file? old-path (string-append (ftp-session-root-dir session)
                                                         curr-dir "/" parent-path) name user))
                (else
                 (print-crlf/encoding* session output-port "550 Can't rename file or directory.")))
              (set-ftp-session-rename-path! session #f))
            (print-crlf/encoding* session output-port "503 Bad sequence of commands."))
        (print-crlf/encoding* session output-port "501 Syntax error in parameters or arguments."))))

;; Инициирует пассивный режим.
(define (PASV-COMMAND params session output-port)
  (if params
      (print-crlf/encoding* session output-port "501 Syntax error in parameters or arguments.")
      (let* ([host-port (ftp-session-passive-host-port session)]
             [host (ftp-passive-host-port-host host-port)])
        (let-values ([(h5 h6) (quotient/remainder current-passive-port 256)])
          (print-crlf/encoding* session output-port
                                (format "227 Entering Passive Mode (~a,~a,~a,~a,~a,~a)"
                                        (bytes-ref host 0) (bytes-ref host 1) (bytes-ref host 2) (bytes-ref host 3)
                                        h5 h6)))
        (set-ftp-passive-host-port-port! host-port current-passive-port)
        (set-ftp-session-transfer-mode! session 'passive)
        (set! current-passive-port (if (>= current-passive-port passive-ports-to)
                                       passive-ports-from
                                       (add1 current-passive-port))))))

(define (init-passive-listeners server-custodian host)
  (parameterize ([current-custodian server-custodian])
    (set! passive-listeners
          (build-vector (- (add1 passive-ports-to) passive-ports-from)
                        (λ (n) (tcp-listen (passive-ports-from . + . n) 1 #t host))))))

(define (get-passive-listener port)
  (vector-ref passive-listeners (- port passive-ports-from)))

(define (init-ftp-dirs root-dir)
  (unless (ftp-dir-exists? root-dir)
    (ftp-mkdir* root-dir)))

(define (ftp-data-transfer session data msg-out [file? #f])
  (case (ftp-session-transfer-mode session)
    ((passive)
     (passive-data-transfer session data msg-out file?))
    ((active)
     (active-data-transfer session data msg-out file?))))

;; Experimental
(define (active-data-transfer session data msg-out file?)
  (let ([host (ftp-active-host-port-host (ftp-session-active-host-port session))]
        [port (ftp-active-host-port-port (ftp-session-active-host-port session))]
        [cust (make-custodian)])
    (set-ftp-session-current-process! session cust)
    (parameterize ([current-custodian cust])
      (thread (λ ()
                (parameterize ([current-custodian cust])
                  (with-handlers ([any/c (λ (e)
                                           (print-crlf/encoding* session msg-out
                                                                 "426 Connection closed; transfer aborted.")
                                           (flush-output msg-out))])
                    (let-values ([(in out) (tcp-connect (byte-host->string host) port)])
                      (let ([reptype (ftp-session-representation-type session)])
                        (print-crlf/encoding* session msg-out
                                              (format "150 Opening ~a mode data connection." reptype))
                        (flush-output msg-out)
                        (if file?
                            (call-with-input-file data
                              (λ (in)
                                (let loop ([dat (read-bytes 10048576 in)])
                                  (unless (eof-object? dat)
                                    (write-bytes dat out)
                                    (loop (read-bytes 10048576 in))))))
                            (case reptype
                              ((ASCII)
                               (print/encoding (ftp-session-locale-encoding session) out data))
                              ((Image)
                               (write-bytes data out))))
                        (flush-output out)
                        (print-crlf/encoding* session msg-out "226 Transfer complete.")
                        (flush-output msg-out))))
                  ;(displayln  "Process complete!" ftp-log-output-port)
                  (custodian-shutdown-all cust))))
      (thread (λ ()
                (sleep 600)
                ;(displayln "Auto kill working process!" ftp-log-output-port)
                (custodian-shutdown-all cust))))))

;; Experimental
(define (passive-data-transfer session data msg-out file?)
  (let ([host (ftp-passive-host-port-host (ftp-session-passive-host-port session))]
        [port (ftp-passive-host-port-port (ftp-session-passive-host-port session))]
        [cust (make-custodian)])
    (set-ftp-session-current-process! session cust)
    (parameterize ([current-custodian cust])
      (thread (λ ()
                (parameterize ([current-custodian cust])
                  (with-handlers ([any/c (λ (e)
                                           (print-crlf/encoding* session msg-out
                                                                 "426 Connection closed; transfer aborted.")
                                           (flush-output msg-out))])
                    (let ([listener (get-passive-listener port)])
                      (let-values ([(in out) (tcp-accept listener)])
                        (let ([reptype (ftp-session-representation-type session)])
                          (print-crlf/encoding* session msg-out
                                                (format "150 Opening ~a mode data connection." reptype))
                          (flush-output msg-out)
                          (if file?
                              (call-with-input-file data
                                (λ (in)
                                  (let loop ([dat (read-bytes 10048576 in)])
                                    (unless (eof-object? dat)
                                      (write-bytes dat out)
                                      (loop (read-bytes 10048576 in))))))
                              (case reptype
                                ((ASCII)
                                 (print/encoding (ftp-session-locale-encoding session) out data))
                                ((Image)
                                 (write-bytes data out))))
                          (flush-output out)
                          (print-crlf/encoding* session msg-out "226 Transfer complete.")
                          (flush-output msg-out)))))
                  ;(displayln "Process complete!" ftp-log-output-port)
                  (custodian-shutdown-all cust))))
      (thread (λ ()
                (sleep 600)
                ;(displayln "Auto kill working process!" ftp-log-output-port)
                (custodian-shutdown-all cust))))))

(define (ftp-store-file session new-file-full-path msg-out exists-mode)
  (case (ftp-session-transfer-mode session)
    ((passive)
     (passive-store-file session new-file-full-path msg-out exists-mode))
    ((active)
     (active-store-file session new-file-full-path msg-out exists-mode))))

;; Experimental
(define (active-store-file session new-file-full-path msg-out exists-mode)
  (let ([host (ftp-active-host-port-host (ftp-session-active-host-port session))]
        [port (ftp-active-host-port-port (ftp-session-active-host-port session))]
        [cust (make-custodian)])
    (set-ftp-session-current-process! session cust)
    (parameterize ([current-custodian cust])
      (thread (λ ()
                (with-handlers ([any/c (λ (e)
                                         (print-crlf/encoding* session msg-out "426 Connection closed; transfer aborted.")
                                         (flush-output msg-out))])
                  (call-with-output-file new-file-full-path
                    (λ (fout)
                      (parameterize ([current-custodian cust])
                        (let-values ([(in out) (tcp-connect (byte-host->string host) port)])
                          (print-crlf/encoding* session msg-out
                                                (format "150 Opening ~a mode data connection."
                                                        (ftp-session-representation-type session)))
                          (flush-output msg-out)
                          (let loop ([dat (read-bytes 10048576 in)])
                            (unless (eof-object? dat)
                              (write-bytes dat fout)
                              (loop (read-bytes 10048576 in))))
                          (flush-output fout)
                          (print-log-event session (format "~a data to file ~a"
                                                           (if (eq? exists-mode 'append) "Append" "Store")
                                                           (real-path->ftp-path session new-file-full-path)))
                          (print-crlf/encoding* session msg-out "226 Transfer complete.")
                          (flush-output msg-out))))
                    #:mode 'binary
                    #:exists exists-mode))
                ;(displayln "Process complete!" ftp-log-output-port)
                (custodian-shutdown-all cust)))
      (thread (λ ()
                (sleep 600)
                ;(displayln "Auto kill working process!" ftp-log-output-port)
                (custodian-shutdown-all cust))))))

;; Experimental
(define (passive-store-file session new-file-full-path msg-out exists-mode)
  (let ([host (ftp-passive-host-port-host (ftp-session-passive-host-port session))]
        [port (ftp-passive-host-port-port (ftp-session-passive-host-port session))]
        [cust (make-custodian)])
    (set-ftp-session-current-process! session cust)
    (parameterize ([current-custodian cust])
      (thread (λ ()
                (with-handlers ([any/c (λ (e)
                                         (print-crlf/encoding* session msg-out "426 Connection closed; transfer aborted.")
                                         (flush-output msg-out))])
                  (call-with-output-file new-file-full-path
                    (λ (fout)
                      (parameterize ([current-custodian cust])
                        (let ([listener (get-passive-listener port)])
                          (let-values ([(in out) (tcp-accept listener)])
                            (print-crlf/encoding* session msg-out
                                                  (format "150 Opening ~a mode data connection."
                                                          (ftp-session-representation-type session)))
                            (flush-output msg-out)
                            (let loop ([dat (read-bytes 10048576 in)])
                              (unless (eof-object? dat)
                                (write-bytes dat fout)
                                (loop (read-bytes 10048576 in))))
                            (flush-output fout)
                            (print-log-event session (format "~a data to file ~a"
                                                             (if (eq? exists-mode 'append) "Append" "Store")
                                                             (real-path->ftp-path session new-file-full-path)))
                            (print-crlf/encoding* session msg-out "226 Transfer complete.")
                            (flush-output msg-out)))))
                    #:mode 'binary
                    #:exists exists-mode))
                ;(displayln "Process complete!" ftp-log-output-port)
                (custodian-shutdown-all cust)))
      (thread (λ ()
                (sleep 600)
                ;(displayln "Auto kill working process!" ftp-log-output-port)
                (custodian-shutdown-all cust))))))

(define (byte-host->string byte-host)
  (string-join (map number->string (bytes->list byte-host))
               "."))

(define (read-request session input-port)
  (if (byte-ready? input-port)
      (let ([line (read-bytes-line input-port)])
        (if (eof-object? line)
            line
            (let ([s (request-bytes->string/encoding (ftp-session-locale-encoding session) line)])
              (substring s 0 (sub1 (string-length s))))))
      #f))

(define (get-params req)
  (get-params* #rx"[^A-z]+.*" req))

(define (get-params* delseq req)
  (let ([p (regexp-match delseq req)])
    (if p
        (let ([p (regexp-match #rx"[^ \t]+.*" (car p))])
          (if p
              (let ([p (regexp-match #rx".*[^ \t]+" (car p))])
                (if p (car p) #f))
              #f))
        #f)))

(define (ftp-file-or-dir-full-info sys-file)
  (call-with-input-file sys-file
    (λ (in)
      (vector (integer-bytes->integer (read-bytes 2 in) #f) ; sysbytes
              (read-line in) ; owner
              (read-line in) ; group
              ))))

(define (ftp-file-or-dir-sysbytes sys-file)
  (call-with-input-file sys-file
    (λ (in)
      (integer-bytes->integer (read-bytes 2 in) #f))))

(define (ftp-file-or-dir-sysbytes/owner sys-file)
  (call-with-input-file sys-file
    (λ (in)
      (vector (integer-bytes->integer (read-bytes 2 in) #f)
              (read-line in)))))

(define (ftp-mkdir* spath [owner "racket"][group "racket"][permissions #b111110100])
  (make-directory spath)
  (ftp-mksys-file (string-append spath "/.ftp-racket-directory")
                  owner group permissions))

(define (ftp-mksys-file sys-file [owner "racket"][group "racket"][permissions #b111110100])
  (call-with-output-file sys-file
    (λ (out)
      [write-bytes (integer->integer-bytes permissions 2 #f) out]
      [display owner out][newline out]
      [display group out][newline out])
    #:exists 'truncate))

(define (ftp-dir-exists? spath)
  (and (directory-exists? spath)
       (file-exists? (string-append spath "/.ftp-racket-directory"))))

(define (ftp-file-exists? spath)
  (and (file-exists? spath)
       (file-exists? (string-append spath ".ftp-racket-file"))))

(define (ftp-file-name-safe? spath)
  (not (and (filename-extension spath)
            (or (bytes=? (filename-extension spath) #"ftp-racket-file")
                (bytes=? (filename-extension spath) #"ftp-racket-directory")))))

(define (real-path->ftp-path session real-path [drop-tail-elem 0])
  (simplify-ftp-path (substring real-path (string-length (ftp-session-root-dir session))) drop-tail-elem))

(define (simplify-ftp-path ftp-path [drop-tail-elem 0])
  (with-handlers ([any/c (λ (e) "/")])
    (let ([path-lst (drop-right (filter (λ (s) (not (string=? s "")))
                                        (regexp-split #rx"[/\\\\]+" (simplify-path ftp-path #f)))
                                drop-tail-elem)])
      (if (null? path-lst)
          "/"
          (foldr (λ (a b) (string-append "/" a b)) "" path-lst)))))

(define (ftp-allow-read? full-ftp-sys-file-spath user)
  (let ([info (ftp-file-or-dir-full-info full-ftp-sys-file-spath)])
    (cond
      ((string=? (vector-ref info 1) (ftp-user-login user))
       (bitwise-bit-set? (vector-ref info 0) 8))
      ((string=? (vector-ref info 2) (ftp-user-group user))
       (bitwise-bit-set? (vector-ref info 0) 5))
      (else
       (bitwise-bit-set? (vector-ref info 0) 2)))))

(define (ftp-allow-write? full-ftp-sys-file-spath user)
  (let ([info (ftp-file-or-dir-full-info full-ftp-sys-file-spath)])
    (cond
      ((string=? (vector-ref info 1) (ftp-user-login user))
       (bitwise-bit-set? (vector-ref info 0) 7))
      ((string=? (vector-ref info 2) (ftp-user-group user))
       (bitwise-bit-set? (vector-ref info 0) 4))
      (else
       (bitwise-bit-set? (vector-ref info 0) 1)))))

(define (ftp-allow-delete-move? full-ftp-sys-file-spath user)
  (call-with-input-file full-ftp-sys-file-spath
    (λ (in)
      (let ([sysbytes (integer-bytes->integer (read-bytes 2 in) #f)]
            [owner (read-line in)])
        (and (string=? owner (ftp-user-login user))
             (bitwise-bit-set? sysbytes 7))))))

(define (ftp-dir-allow-read? spath user)
  (ftp-allow-read? (string-append spath "/.ftp-racket-directory") user))

(define (ftp-dir-allow-write? spath user)
  (ftp-allow-write? (string-append spath "/.ftp-racket-directory") user))

(define (ftp-dir-allow-delete-move? spath user)
  (ftp-allow-delete-move? (string-append spath "/.ftp-racket-directory") user))

(define (ftp-file-allow-read? spath user)
  (ftp-allow-read? (string-append spath ".ftp-racket-file") user))

(define (ftp-file-allow-write? spath user)
  (ftp-allow-write? (string-append spath ".ftp-racket-file") user))

(define (ftp-file-allow-delete-move? spath user)
  (ftp-allow-delete-move? (string-append spath ".ftp-racket-file") user))

(define print-log-event
  (let ()
    (date-display-format 'iso-8601)
    (λ (session msg [user-name? #t])
      (if user-name?
          (fprintf ftp-log-output-port
                   "~a [~a] ~a : ~a\n"
                   (date->string (current-date) #t) (ftp-session-client-host session)
                   (ftp-session-user-id session) msg)
          (fprintf ftp-log-output-port
                   "~a [~a] ~a\n"
                   (date->string (current-date) #t) (ftp-session-client-host session) msg)))))

(define (print-crlf/encoding* session out text)
  (print/encoding (ftp-session-locale-encoding session) out text)
  (write-bytes #"\r\n" out))

(define (print-crlf/encoding encoding out text)
  (print/encoding encoding out text)
  (write-bytes #"\r\n" out))

(define (print/encoding encoding out text)
  (if (string=? encoding "UTF-8")
      (display text out)
      (let ([conv (bytes-open-converter "UTF-8" encoding)])
        (let-values ([(bstr len result) (bytes-convert conv (string->bytes/utf-8 text))])
          (bytes-close-converter conv)
          (write-bytes bstr out)))))

(define (request-bytes->string/encoding encoding bstr)
  (if (string=? encoding "UTF-8")
      (bytes->string/utf-8 bstr)
      (let ([conv (bytes-open-converter encoding "UTF-8")])
        (let-values ([(bstr len result) (bytes-convert conv bstr)])
          (bytes-close-converter conv)
          (bytes->string/utf-8 bstr)))))

(define (list-string->bytes/encoding encoding str)
  (if (string=? encoding "UTF-8")
      (string->bytes/utf-8 str)
      (let ([conv (bytes-open-converter "UTF-8" encoding)])
        (let-values ([(bstr len result) (bytes-convert conv (string->bytes/utf-8 str))])
          (bytes-close-converter conv)
          bstr))))

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

(define (mlst-info session ftp-path [full-path? #t])
  (let* ([ftp-path (simplify-ftp-path ftp-path)]
         [path (string-append (ftp-session-root-dir session) ftp-path)]
         [name (path->string (file-name-from-path ftp-path))]
         [file? (file-exists? path)]
         [info (ftp-file-or-dir-full-info (string-append path
                                                         (if file?
                                                             ".ftp-racket-file"
                                                             "/.ftp-racket-directory")))]
         [sysbytes (vector-ref info 0)]
         [user (current-ftp-user session)]
         [user-name (ftp-user-login user)]
         [group (ftp-user-group user)]
         [features (ftp-session-mlst-features session)])
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

(define (current-ftp-user session)
  (hash-ref ftp-users (ftp-session-user-id session)))

(define (kill-current-ftp-process session)
  (custodian-shutdown-all (ftp-session-current-process session)))

(define (add-ftp-user full-name login pass group home-dirs [root-dir ftp-root-dir])
  (let ([root-dir (get-params* #rx".+" root-dir)])
    (hash-set! ftp-users login (ftp-user full-name login pass group home-dirs root-dir))
    (init-ftp-dirs root-dir)
    (for-each (λ (home-dir)
                (unless (ftp-dir-exists? (string-append root-dir home-dir))
                  (let ([dirs (filter (λ (s) (not (string=? s "")))
                                      (regexp-split #rx"[/\\\\]+" home-dir))]
                        [curr-dir ""])
                    (unless (zero? (length dirs))
                      (for-each (λ (p)
                                  (unless (ftp-dir-exists? (string-append root-dir curr-dir "/" p))
                                    (ftp-mkdir* (string-append root-dir curr-dir "/" p)))
                                  (set! curr-dir (string-append curr-dir "/" p)))
                                (drop-right dirs 1)))
                    (ftp-mkdir* (string-append root-dir home-dir) login group))))
              home-dirs)))

(define (set-default-locale-encoding encoding)
  (set! ftp-default-locale-encoding encoding))

(define (set-ftp-root-dir name)
  (set! ftp-root-dir name))

(define (set-log-output-port output-port)
  (set! ftp-log-output-port output-port))

(define (set-passive-ports from to)
  (set! passive-ports-from from)
  (set! passive-ports-to to))

