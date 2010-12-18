#|

Racket FTP Server Library v1.0.17
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
         srfi/48)

(provide ftp-server%)

(struct ftp-user (full-name login pass group home-dirs root-dir))
(struct ftp-active-host-port (host port))
(struct ftp-passive-host-port (host port) #:mutable)
(struct ftp-mlst-features (size? modify? perm?) #:mutable)

(struct ftp-server-params 
  (passive-ports-from
   passive-ports-to
   current-passive-port
   
   passive-listeners
   
   default-root-dir
   
   default-locale-encoding
   
   log-output-port
   
   ftp-users)
  #:mutable)

(date-display-format 'iso-8601)

;;
;; ---------- Global Definitions ----------
;;
(define ftp-run-date (srfi/19:current-date))
(define ftp-date-zone-offset (srfi/19:date-zone-offset ftp-run-date))

(define dead-process (make-custodian))

(define (default-server-params)
  (ftp-server-params 
   40000                 ;passive-ports-from
   40599                 ;passive-ports-to
   40000                 ;current-passive-port
   
   #f                    ;passive-listeners
   
   "ftp-dir"             ;default-root-dir
   
   "UTF-8"               ;default-locale-encoding
   
   (current-output-port) ;log-output-port
   
   (make-hash)           ;ftp-users
   ))


(define ftp-utils%
  (class object%
    
    (define/public (get-params req)
      (get-params* #rx"[^A-z]+.*" req))
    
    (define/public (get-params* delseq req)
      (let ([p (regexp-match delseq req)])
        (if p
            (let ([p (regexp-match #rx"[^ \t]+.*" (car p))])
              (if p
                  (let ([p (regexp-match #rx".*[^ \t]+" (car p))])
                    (if p (car p) #f))
                  #f))
            #f)))
    
    (define/public (ftp-file-or-dir-full-info sys-file)
      (call-with-input-file sys-file
        (λ (in)
          (vector (integer-bytes->integer (read-bytes 2 in) #f) ; sysbytes
                  (read-line in) ; owner
                  (read-line in) ; group
                  ))))
    
    (define/public (ftp-file-or-dir-sysbytes sys-file)
      (call-with-input-file sys-file
        (λ (in)
          (integer-bytes->integer (read-bytes 2 in) #f))))
    
    (define/public (ftp-file-or-dir-sysbytes/owner sys-file)
      (call-with-input-file sys-file
        (λ (in)
          (vector (integer-bytes->integer (read-bytes 2 in) #f)
                  (read-line in)))))
    
    (define/public (ftp-mkdir* spath [owner "racket"][group "racket"][permissions #b111110100])
      (make-directory spath)
      (ftp-mksys-file (string-append spath "/.ftp-racket-directory")
                      owner group permissions))
    
    (define/public (ftp-mksys-file sys-file [owner "racket"][group "racket"][permissions #b111110100])
      (call-with-output-file sys-file
        (λ (out)
          [write-bytes (integer->integer-bytes permissions 2 #f) out]
          [display owner out][newline out]
          [display group out][newline out])
        #:exists 'truncate))
    
    (define/public (ftp-dir-exists? spath)
      (and (directory-exists? spath)
           (file-exists? (string-append spath "/.ftp-racket-directory"))))
    
    (define/public (ftp-file-exists? spath)
      (and (file-exists? spath)
           (file-exists? (string-append spath ".ftp-racket-file"))))
    
    (define/public (ftp-file-name-safe? spath)
      (not (and (filename-extension spath)
                (or (bytes=? (filename-extension spath) #"ftp-racket-file")
                    (bytes=? (filename-extension spath) #"ftp-racket-directory")))))
    
    (define/public (simplify-ftp-path ftp-path [drop-tail-elem 0])
      (with-handlers ([any/c (λ (e) "/")])
        (let ([path-lst (drop-right (filter (λ (s) (not (string=? s "")))
                                            (regexp-split #rx"[/\\\\]+" (simplify-path ftp-path #f)))
                                    drop-tail-elem)])
          (if (null? path-lst)
              "/"
              (foldr (λ (a b) (string-append "/" a b)) "" path-lst)))))
    
    (define/public (ftp-allow-read? full-ftp-sys-file-spath user)
      (let ([info (ftp-file-or-dir-full-info full-ftp-sys-file-spath)])
        (cond
          ((string=? (vector-ref info 1) (ftp-user-login user))
           (bitwise-bit-set? (vector-ref info 0) 8))
          ((string=? (vector-ref info 2) (ftp-user-group user))
           (bitwise-bit-set? (vector-ref info 0) 5))
          (else
           (bitwise-bit-set? (vector-ref info 0) 2)))))
    
    (define/public (ftp-allow-write? full-ftp-sys-file-spath user)
      (let ([info (ftp-file-or-dir-full-info full-ftp-sys-file-spath)])
        (cond
          ((string=? (vector-ref info 1) (ftp-user-login user))
           (bitwise-bit-set? (vector-ref info 0) 7))
          ((string=? (vector-ref info 2) (ftp-user-group user))
           (bitwise-bit-set? (vector-ref info 0) 4))
          (else
           (bitwise-bit-set? (vector-ref info 0) 1)))))
    
    (define/public (ftp-allow-delete-move? full-ftp-sys-file-spath user)
      (call-with-input-file full-ftp-sys-file-spath
        (λ (in)
          (let ([sysbytes (integer-bytes->integer (read-bytes 2 in) #f)]
                [owner (read-line in)])
            (and (string=? owner (ftp-user-login user))
                 (bitwise-bit-set? sysbytes 7))))))
    
    (define/public (ftp-dir-allow-read? spath user)
      (ftp-allow-read? (string-append spath "/.ftp-racket-directory") user))
    
    (define/public (ftp-dir-allow-write? spath user)
      (ftp-allow-write? (string-append spath "/.ftp-racket-directory") user))
    
    (define/public (ftp-dir-allow-delete-move? spath user)
      (ftp-allow-delete-move? (string-append spath "/.ftp-racket-directory") user))
    
    (define/public (ftp-file-allow-read? spath user)
      (ftp-allow-read? (string-append spath ".ftp-racket-file") user))
    
    (define/public (ftp-file-allow-write? spath user)
      (ftp-allow-write? (string-append spath ".ftp-racket-file") user))
    
    (define/public (ftp-file-allow-delete-move? spath user)
      (ftp-allow-delete-move? (string-append spath ".ftp-racket-file") user))
    
    ;(define/public (print-crlf/encoding encoding out text)
    ;  (print/encoding encoding out text)
    ;  (write-bytes #"\r\n" out))
    
    (define/public (print/encoding encoding out text)
      (let ([conv (bytes-open-converter "UTF-8" encoding)])
        (let-values ([(bstr len result) (bytes-convert conv (string->bytes/utf-8 text))])
          (bytes-close-converter conv)
          (write-bytes bstr out))))
    
    (define/public (request-bytes->string/encoding encoding bstr)
      (let ([conv (bytes-open-converter encoding "UTF-8")])
        (let-values ([(bstr len result) (bytes-convert conv bstr)])
          (bytes-close-converter conv)
          (bytes->string/utf-8 bstr))))
    
    (define/public (list-string->bytes/encoding encoding str)
      (let ([conv (bytes-open-converter "UTF-8" encoding)])
        (let-values ([(bstr len result) (bytes-convert conv (string->bytes/utf-8 str))])
          (bytes-close-converter conv)
          bstr)))
    
    (super-new)))

(define ftp-client%
  (class ftp-utils%
    (inherit get-params
             print/encoding
             list-string->bytes/encoding
             request-bytes->string/encoding
             ftp-file-or-dir-full-info
             simplify-ftp-path
             ftp-file-name-safe?
             ftp-file-exists?
             ftp-file-allow-read?
             ftp-file-allow-write?
             ftp-file-allow-delete-move?
             ftp-dir-exists?
             ftp-dir-allow-read?
             ftp-dir-allow-write?
             ftp-dir-allow-delete-move?
             ftp-file-or-dir-sysbytes/owner
             ftp-mkdir*
             ftp-mksys-file)
    ;;
    ;; ---------- Public Definitions ----------
    ;;
    (init-field client-host
                client-input-port
                client-output-port
                [server-params (default-server-params)])
    ;;
    ;; ---------- Private Definitions ----------
    ;;
    (define user-id #f)
    (define user-logged #f)
    (define root-dir default-root-dir)
    (define current-dir "/")
    (define DTP 'active)
    (define transfer-mode 'Stream)
    (define representation-type 'ASCII)
    (define file-structure 'File)
    (define restart-marker #f)
    (define active-host-port (ftp-active-host-port (bytes 127 0 0 1) 20))
    (define passive-host-port (ftp-passive-host-port (bytes 127 0 0 1) 20))
    (define current-process dead-process)
    (define rename-path #f)
    (define locale-encoding default-locale-encoding)
    (define mlst-features (ftp-mlst-features #t #t #f))
    (define lang-list '(EN RU))
    (define current-lang 'EN)
    
    (define print/locale-encoding #f)
    (define request-bytes->string/locale-encoding #f)
    (define list-string->bytes/locale-encoding #f)
    (define cmd-list null)
    (define cmd-voc #f)
    (define server-responses #f)
    ;;
    ;; ---------- Public Methods ----------
    ;;
    (define/public (eval-client-request [reset-timer void])
      (with-handlers ([any/c #|displayln|# void])
        (print-crlf/encoding** 'WELCOME)
        ;(sleep 1)
        (let loop ([request (read-request client-input-port)])
          (unless (eof-object? request)
            (when request
              ;(printf "[~a] ~a\n" client-host request)
              (let ([cmd (string-upcase (car (regexp-match #rx"[^ ]+" request)))]
                    [params (get-params request)])
                (if user-logged
                    (let ([rec (hash-ref cmd-voc cmd #f)])
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
            (loop (read-request client-input-port)))))
      (flush-output client-output-port))
    ;;
    ;; ---------- Private Methods ----------
    ;;
    ;; Возвращает идентификатор(имя) пользователя
    (define (USER-COMMAND params)
      (if params
          (let ([name params])
            (if (and (hash-ref ftp-users name #f)
                     (string=? (ftp-user-pass (hash-ref ftp-users name)) ""))
                (print-crlf/encoding** 'ANONYMOUS-LOGIN)
                (print-crlf/encoding** 'PASSW-REQUIRED name))
            (set! user-id name))
          (begin
            (print-crlf/encoding** 'SYNTAX-ERROR "")
            (set! user-id #f)))
      (set! user-logged #f))
    
    ;; Проверяет пороль пользователя
    (define (PASS-COMMAND params)
      (let ([correct?
             (cond
               ((string? user-id)
                (let ([pass params])
                  (cond
                    ((not (hash-ref ftp-users user-id #f))
                     (print-log-event "Login incorrect.")
                     (print-crlf/encoding** 'LOGIN-INCORRECT)
                     #f)
                    ((string=? (ftp-user-pass (hash-ref ftp-users user-id))
                               "")
                     (print-log-event "User logged in.")
                     (print-crlf/encoding** 'ANONYMOUS-LOGGED)
                     #t)
                    ((not pass)
                     (print-log-event "Login incorrect.")
                     (print-crlf/encoding** 'LOGIN-INCORRECT)
                     #f)
                    ((string=? (ftp-user-pass (hash-ref ftp-users user-id))
                               pass)
                     (print-log-event "User logged in.")
                     (print-crlf/encoding** 'USER-LOGGED user-id)
                     #t)
                    (else
                     (print-log-event "Password incorrect.")
                     (print-crlf/encoding** 'LOGIN-INCORRECT)
                     #f))))
               (else
                (print-crlf/encoding** 'LOGIN-INCORRECT)
                #f))])
        (set! user-logged correct?)
        (when correct?
          (set! root-dir (ftp-user-root-dir (hash-ref ftp-users user-id))))))
    
    (define (REIN-COMMAND params)
      (if params
          (print-crlf/encoding** 'SYNTAX-ERROR "")
          (begin
            (set! user-id #f)
            (set! user-logged #f)
            (print-crlf/encoding** 'SERVICE-READY))))
    
    (define (QUIT-COMMAND params)
      (if params
          (print-crlf/encoding** 'SYNTAX-ERROR "")
          (begin
            (kill-current-ftp-process)
            (print-crlf/encoding** 'QUIT)
            (raise 'quit))))
    
    (define (PWD-COMMAND params)
      (if params
          (print-crlf/encoding** 'SYNTAX-ERROR "")
          (print-crlf/encoding** 'CURRENT-DIR current-dir)))
    
    (define (CDUP-COMMAND params)
      (if params
          (print-crlf/encoding** 'SYNTAX-ERROR "")
          (begin
            (set! current-dir (simplify-ftp-path current-dir 1))
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
                                                                      (if (eq? l current-lang) "*" ""))) 
                                                     lang-list)
                                                ";")))
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
    
    ;; Инициирует активный режим.
    (define (PORT-COMMAND params)
      (with-handlers ([any/c (λ (e) (print-crlf/encoding** 'SYNTAX-ERROR ""))])
        (unless (regexp-match #rx"^[0-9]+,[0-9]+,[0-9]+,[0-9]+,[0-9]+,[0-9]+$" params)
          (raise 'error))
        (let* ([l (regexp-split #rx"," params)]
               [host (bytes (string->number (first l)) (string->number (second l))
                            (string->number (third l)) (string->number (fourth l)))]
               [port (((string->number (fifth l)). * . 256). + .(string->number (sixth l)))])
          (when (port . > . 65535) (raise 'error))
          (set! DTP 'active)
          (set! active-host-port (ftp-active-host-port host port))
          (print-crlf/encoding** 'CMD-SUCCESSFUL 200 "PORT"))))
    
    ;; Experimental
    (define (REST-COMMAND params)
      (with-handlers ([any/c (λ (e) (print-crlf/encoding** 'SYNTAX-ERROR ""))])
        (set! restart-marker (string->number (car (regexp-match #rx"^[0-9]+$" params))))
        (print-crlf/encoding** 'RESTART)))
    
    ;; Experimental
    (define (ALLO-COMMAND params)
      (if (params . and .(regexp-match #rx"^[0-9]+$" params))
          (print-crlf/encoding** 'CMD-SUCCESSFUL 200 "ALLO")
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    ;; Experimental
    (define (STAT-COMMAND params)
      (if params
          (begin
            (print-crlf/encoding** 'STATUS-LIST params)
            (DIR-LIST params #f #t)
            (print-crlf/encoding** 'END 213))
          (begin
            (print-crlf/encoding** 'STATUS-INFO-1)
            (print-crlf/encoding** 'STATUS-INFO-2 client-host)
            (print-crlf/encoding** 'STATUS-INFO-3 user-id)
            (print-crlf/encoding** 'STATUS-INFO-4 representation-type file-structure transfer-mode)
            ; Print status of the operation in progress?
            (print-crlf/encoding** 'END 211))))
    
    ;; Experimental
    (define (LANG-COMMAND params)
      (if params
          (let ([lang (string->symbol (string-upcase params))])
            (if (memq lang lang-list)
                (begin
                  (set! current-lang lang)
                  (print-crlf/encoding** 'SET-CMD 200 "LANG" current-lang))
                (print-crlf/encoding** 'MISSING-PARAMS)))
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    ;; Посылает клиенту список файлов директории.
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
           (let ([info (read-ftp-sys-info ftp-path (string-append spath "/.ftp-racket-directory"))])
             (string-append "d" (car info) (second info) (third info) " 2 " (fourth info) " " (fifth info))))
         
         (define (read-ftp-file-sys-info ftp-path spath)
           (let ([info (read-ftp-sys-info ftp-path (string-append spath ".ftp-racket-file"))])
             (string-append "-" (car info) (second info) (third info) " 1 " (fourth info) " " (fifth info))))
         
         (define (dlst ftp-dir-name)
           (let* ([full-dir-name (string-append root-dir ftp-dir-name)]
                  [dirlist (string-append*
                            (map (λ (p)
                                   (let* ([spath (path->string p)]
                                          [ftp-full-spath (string-append (simplify-ftp-path ftp-dir-name) "/" spath)]
                                          [full-spath (string-append full-dir-name "/" spath)])
                                     (if (or (and (ftp-file-exists? full-spath)
                                                  (ftp-file-allow-read? full-spath current-ftp-user))
                                             (and (ftp-dir-exists? full-spath)
                                                  (ftp-dir-allow-read? full-spath current-ftp-user)))
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
             (if status
                 (print-crlf/encoding* dirlist)
                 (ftp-data-transfer (case representation-type
                                      ((ASCII) dirlist)
                                      ((Image) (list-string->bytes/locale-encoding dirlist)))))))]
        
        (let ([dir (if (and params (eq? (string-ref params 0) #\-))
                       (substring (car (regexp-match #px"[^-\\w][^ \t-]+.*" params)) 1)
                       params)])
          (cond
            ((not dir)
             (dlst current-dir))
            ((memq (string-ref dir 0) '(#\/ #\\))
             (if (ftp-dir-exists? (string-append root-dir dir))
                 (dlst dir)
                 (unless status
                   (print-crlf/encoding** 'DIR-NOT-FOUND))))
            ((ftp-dir-exists? (string-append root-dir current-dir "/" dir))
             (dlst (string-append current-dir "/" dir)))
            (else
             (unless status
               (print-crlf/encoding** 'DIR-NOT-FOUND)))))))
    
    ;; Отправляет клиенту копию файла.
    (define (RETR-COMMAND params)
      (local [(define (fcopy full-path-file)
                (ftp-data-transfer full-path-file #t))]
        
        (cond
          ((not params)
           (print-crlf/encoding** 'SYNTAX-ERROR ""))
          ((and (memq (string-ref params 0) '(#\/ #\\))
                (ftp-file-exists? (string-append root-dir params)))
           (if (ftp-file-allow-read? (string-append root-dir params)
                                     current-ftp-user)
               (fcopy (string-append root-dir params))
               (print-crlf/encoding** 'PERM-DENIED)))
          ((ftp-file-exists? (string-append root-dir current-dir "/" params))
           (if (ftp-file-allow-read? (string-append root-dir current-dir "/" params)
                                     current-ftp-user)
               (fcopy (string-append root-dir current-dir "/" params))
               (print-crlf/encoding** 'PERM-DENIED)))
          (else
           (print-crlf/encoding** 'DIR-NOT-FOUND)))))
    
    ;; Устанавливает тип передачи файла (реализован только A(текстовый) и I(бинарный)).
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
    
    ;; Experimental
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
    
    ;; Experimental
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
    
    ;; Изменяет рабочую директорию (current-dir).
    (define (CWD-COMMAND params)
      (cond
        ((not params)
         (print-crlf/encoding** 'SYNTAX-ERROR ""))
        ((and (memq (string-ref params 0) '(#\/ #\\))
              (ftp-dir-exists? (string-append root-dir params)))
         (set! current-dir (simplify-ftp-path params))
         (print-crlf/encoding** 'CMD-SUCCESSFUL 250 "CWD"))
        ((ftp-dir-exists? (string-append root-dir current-dir "/" params))
         (set! current-dir (simplify-ftp-path (string-append current-dir "/" params)))
         (print-crlf/encoding** 'CMD-SUCCESSFUL 250 "CWD"))
        (else
         (print-crlf/encoding** 'DIR-NOT-FOUND))))
    
    ;; Создает каталог.
    (define (MKD-COMMAND params)
      (local [(define (mkd ftp-parent-path dir-name user)
                (let* ([full-parent-path (string-append root-dir ftp-parent-path)]
                       [sp (string-append full-parent-path "/" dir-name)])
                  (if (ftp-dir-allow-write? full-parent-path user)
                      (if (ftp-dir-exists? sp)
                          (print-crlf/encoding** 'DIR-EXIST)
                          (let* ([fpp (simplify-ftp-path ftp-parent-path)]
                                 [fp (string-append fpp (if (string=? fpp "/") "" "/") dir-name)])
                            (ftp-mkdir* sp (ftp-user-login user) (ftp-user-group user))
                            (print-log-event (format "Make directory ~a" fp))
                            (print-crlf/encoding** 'DIR-CREATED fp)))
                      (print-crlf/encoding** 'DIR-PERM-DENIED))))]
        
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
                   [user current-ftp-user])
              (cond
                ((not dir-name)
                 (print-crlf/encoding** 'CANT-CREATE-DIR))
                ((and (not parent-path)
                      (ftp-dir-exists? (string-append root-dir current-dir "/" dir-name)))
                 (print-crlf/encoding** 'DIR-EXIST))
                ((not parent-path)
                 (mkd current-dir dir-name user))
                ((memq (string-ref parent-path 0) '(#\/ #\\))
                 (if (ftp-dir-exists? (string-append root-dir parent-path))
                     (mkd parent-path dir-name user)
                     (print-crlf/encoding** 'CANT-CREATE-DIR)))
                ((ftp-dir-exists? (string-append root-dir current-dir "/" parent-path))
                 (mkd (string-append current-dir "/" parent-path) dir-name user))
                (else
                 (print-crlf/encoding** 'CANT-CREATE-DIR))))
            (print-crlf/encoding** 'SYNTAX-ERROR ""))))
    
    ;; Удаляет каталог.
    (define (RMD-COMMAND params)
      (local
        [(define (rmd ftp-path)
           (let ([spath (string-append root-dir ftp-path)])
             (if ((file-or-directory-identity spath). = .(file-or-directory-identity root-dir))
                 (print-crlf/encoding** 'DIR-NOT-FOUND)
                 (if (ftp-dir-allow-delete-move? spath current-ftp-user)
                     (let ([lst (directory-list spath)])
                       (if (> (length lst) 1)
                           (print-crlf/encoding** 'DELDIR-NOT-EMPTY)
                           (with-handlers ([exn:fail:filesystem? (λ (e)
                                                                   (print-crlf/encoding* "550 System error."))])
                             (delete-file (string-append spath "/.ftp-racket-directory"))
                             (delete-directory spath)
                             (print-log-event (format "Remove a directory ~a" (simplify-ftp-path ftp-path)))
                             (print-crlf/encoding** 'CMD-SUCCESSFUL 250 "RMD"))))
                     (print-crlf/encoding** 'DELDIR-PERM-DENIED)))))]
        
        (cond
          ((and (memq (string-ref params 0) '(#\/ #\\))
                (ftp-dir-exists? (string-append root-dir params)))
           (rmd params))
          ((ftp-dir-exists? (string-append root-dir current-dir "/" params))
           (rmd (string-append current-dir "/" params)))
          (else
           (print-crlf/encoding** 'DIR-NOT-FOUND)))))
    
    ;; Сохраняет файл.
    (define (STORE-FILE params [exists-mode 'truncate])
      (local [(define (stor full-parent-path file-name)
                (let ([real-path (string-append full-parent-path "/" file-name)])
                  (if (and (ftp-dir-allow-write? full-parent-path current-ftp-user)
                           (ftp-file-name-safe? real-path)
                           (if (ftp-file-exists? real-path)
                               (ftp-file-allow-write? real-path current-ftp-user)
                               #t))
                      (ftp-store-file real-path exists-mode)
                      (print-crlf/encoding** 'STORE-FILE-PERM-DENIED))))]
        
        (if params
            (let* ([file-name (if (file-name-from-path params)
                                  (path->string (file-name-from-path params))
                                  #f)]
                   [parent-path (if (and file-name (path-only params))
                                    (path->string (path-only params))
                                    #f)])
              (cond
                ((not file-name)
                 (print-crlf/encoding** 'CANT-STORE-FILE))
                ((not parent-path)
                 (stor (string-append root-dir current-dir) file-name))
                ((and (memq (string-ref parent-path 0) '(#\/ #\\))
                      (ftp-dir-exists? (string-append root-dir parent-path)))
                 (stor (string-append root-dir parent-path) file-name))
                ((ftp-dir-exists? (string-append root-dir current-dir "/" parent-path))
                 (stor (string-append root-dir current-dir "/" parent-path) file-name))
                (else
                 (print-crlf/encoding** 'CANT-STORE-FILE))))
            (print-crlf/encoding** 'SYNTAX-ERROR ""))))
    
    ;; Experimental
    (define (STOU-FILE params)
      (cond
        (params
         (print-crlf/encoding** 'SYNTAX-ERROR ""))
        ((ftp-dir-allow-write? (string-append root-dir current-dir) current-ftp-user)
         (let* ([file-name (let loop ([fname (gensym "noname")])
                             (if (ftp-file-exists? (string-append root-dir current-dir "/" fname))
                                 (loop (gensym "noname"))
                                 fname))]
                [path (string-append root-dir current-dir "/" file-name)])
           (ftp-store-file path 'truncate)))
        (else
         (print-crlf/encoding** 'STORE-FILE-PERM-DENIED))))
    
    ;; Удаляет файл.
    (define (DELE-COMMAND params)
      (local
        [(define (dele ftp-path)
           (let ([spath (string-append root-dir ftp-path)])
             (if (ftp-file-allow-delete-move? spath current-ftp-user)
                 (with-handlers ([exn:fail:filesystem? (λ (e)
                                                         (print-crlf/encoding* "550 System error."))])
                   (delete-file (string-append spath ".ftp-racket-file"))
                   (delete-file spath)
                   (print-log-event (format "Delete a file ~a" (simplify-ftp-path ftp-path)))
                   (print-crlf/encoding** 'CMD-SUCCESSFUL 250 "DELE"))
                 (print-crlf/encoding** 'DELFILE-PERM-DENIED))))]
        
        (cond
          ((and (memq (string-ref params 0) '(#\/ #\\))
                (ftp-file-exists? (string-append root-dir params)))
           (dele params))
          ((ftp-file-exists? (string-append root-dir current-dir "/" params))
           (dele (string-append current-dir "/" params)))
          (else
           (print-crlf/encoding** 'FILE-NOT-FOUND)))))
    
    ;; Эмулирует Unix команды.
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
                  [fchmod (λ (full-path target)
                            (let ([owner (vector-ref (ftp-file-or-dir-sysbytes/owner (string-append full-path target)) 1)]
                                  [user current-ftp-user])
                              (if (string=? owner (ftp-user-login user))
                                  (begin
                                    (ftp-mksys-file (string-append full-path target)
                                                    (ftp-user-login user) (ftp-user-group user)
                                                    (get-permis permis))
                                    (print-crlf/encoding** 'CMD-SUCCESSFUL 200 "SITE CHMOD"))
                                  (print-crlf/encoding** 'PERM-DENIED))))])
             (cond
               ((memq (string-ref path 0) '(#\/ #\\))
                (cond
                  ((ftp-file-exists? (string-append root-dir path))
                   (fchmod (string-append root-dir path) ".ftp-racket-file"))
                  ((ftp-dir-exists? (string-append root-dir path))
                   (fchmod (string-append root-dir path) "/.ftp-racket-directory"))
                  (else
                   (print-crlf/encoding** 'FILE-DIR-NOT-FOUND))))
               ((ftp-file-exists? (string-append root-dir current-dir "/" path))
                (fchmod (string-append root-dir
                                       current-dir "/" path) ".ftp-racket-file"))
               ((ftp-dir-exists? (string-append root-dir current-dir "/" path))
                (fchmod (string-append root-dir
                                       current-dir "/" path) "/.ftp-racket-directory"))
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
    
    ;; Experimental
    (define (MDTM-COMMAND params)
      (if params
          (let ([mdtm (λ (path)
                        (if (or (ftp-file-exists? path)
                                (ftp-dir-exists? path))
                            (print-crlf/encoding* (format "213 ~a"
                                                          (seconds->mdtm-time-format
                                                           (file-or-directory-modify-seconds path))))
                            (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)))])
            (if (memq (string-ref params 0) '(#\/ #\\))
                (mdtm (string-append root-dir params))
                (mdtm (string-append root-dir current-dir "/" params))))
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    ;; Experimental
    (define (SIZE-COMMAND params)
      (if params
          (let ([size (λ (path)
                        (if (ftp-file-exists? path)
                            (print-crlf/encoding* (format "213 ~a" (file-size path)))
                            (print-crlf/encoding** 'FILE-NOT-FOUND)))])
            (if (memq (string-ref params 0) '(#\/ #\\))
                (size (string-append root-dir params))
                (size (string-append root-dir current-dir "/" params))))
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    ;; Experimental
    (define (MLST-COMMAND params)
      (local [(define (mlst session ftp-path)
                (print-crlf/encoding** 'MLST-LISTING)
                (print-crlf/encoding* (string-append " " (mlst-info session ftp-path)))
                (print-crlf/encoding** 'END 250))]
        (cond
          ((not params)
           (let ([path (string-append root-dir current-dir)])
             (if ((file-or-directory-identity path). = .(file-or-directory-identity root-dir))
                 (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)
                 (mlst current-dir))))
          ((memq (string-ref params 0) '(#\/ #\\))
           (let ([path (string-append root-dir params)])
             (cond
               ((ftp-file-exists? path)
                (mlst params))
               ((ftp-dir-exists? path)
                (if ((file-or-directory-identity path). = .(file-or-directory-identity root-dir))
                    (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)
                    (mlst params)))
               (else
                (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)))))
          (else
           (let ([path (string-append root-dir current-dir "/" params)])
             (cond
               ((ftp-file-exists? path)
                (mlst (string-append current-dir "/" params)))
               ((ftp-dir-exists? path)
                (if ((file-or-directory-identity path). = .(file-or-directory-identity root-dir))
                    (print-crlf/encoding** 'FILE-DIR-NOT-FOUND)
                    (mlst (string-append current-dir "/" params))))
               (else
                (print-crlf/encoding** 'FILE-DIR-NOT-FOUND))))))))
    
    ;; Experimental
    (define (MLSD-COMMAND params)
      (local [(define (mlsd ftp-path)
                (let* ([path (string-append root-dir ftp-path)]
                       [dirlist (string-append*
                                 (map (λ (p)
                                        (let* ([spath (path->string p)]
                                               [full-spath (string-append path "/" spath)])
                                          (if (or (and (ftp-file-exists? full-spath)
                                                       (ftp-file-allow-read? full-spath current-ftp-user))
                                                  (and (ftp-dir-exists? full-spath)
                                                       (ftp-dir-allow-read? full-spath current-ftp-user)))
                                              (string-append (mlst-info (string-append ftp-path "/" spath) #f)
                                                             "\n")
                                              "")))
                                      (directory-list path)))])
                  (ftp-data-transfer (case representation-type
                                       ((ASCII) dirlist)
                                       ((Image) (list-string->bytes/locale-encoding dirlist))))))]
        (cond
          ((not params)
           (mlsd current-dir))
          ((memq (string-ref params 0) '(#\/ #\\))
           (let ([path (string-append root-dir params)])
             (if (ftp-dir-exists? path)
                 (mlsd params)
                 (print-crlf/encoding** 'DIR-NOT-FOUND))))
          (else
           (let ([path (string-append root-dir current-dir "/" params)])
             (if (ftp-dir-exists? path)
                 (mlsd (string-append current-dir "/" params))
                 (print-crlf/encoding** 'DIR-NOT-FOUND)))))))
    
    
    ;; Experimental
    (define (OPTS-COMMAND params)
      (if params
          (let ([cmd (string->symbol (string-upcase (car (regexp-match #rx"[^ ]+" params))))])
            (case cmd
              ((UTF8)
               (let ([mode (regexp-match #rx"[^ \t]+.+" (substring params 4))])
                 (if mode
                     (case (string->symbol (string-upcase (car mode)))
                       ((ON)
                        (set! locale-encoding "UTF-8")
                        (print-crlf/encoding** 'UTF8-ON))
                       ((OFF)
                        (set! locale-encoding default-locale-encoding)
                        (print-crlf/encoding** 'UTF8-OFF))
                       (else
                        (print-crlf/encoding** 'SYNTAX-ERROR "UTF8:")))
                     (print-crlf/encoding** 'SYNTAX-ERROR "UTF8:"))
                 (release-encoding-proc)))
              ((MLST)
               (let ([modes (regexp-match #rx"[^ \t]+.+" (substring params 4))])
                 (if modes
                     (let ([mlst (map string->symbol
                                      (filter (λ (s) (not (string=? s "")))
                                              (regexp-split #rx"[; \t]+" (string-upcase (car modes)))))])
                       (if (andmap (λ (mode) (member mode '(SIZE MODIFY PERM))) mlst)
                           (let ([features mlst-features])
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
    
    ;; Подготавливает к переименованию файл или каталог.
    (define (RNFR-COMMAND params)
      (if params
          (let ([path1 (string-append root-dir params)]
                [path2 (string-append root-dir current-dir "/" params)])
            (cond
              ((memq (string-ref params 0) '(#\/ #\\))
               (cond
                 ((ftp-file-exists? path1)
                  (if (ftp-file-allow-delete-move? path1 current-ftp-user)
                      (begin
                        (set! rename-path path1)
                        (print-crlf/encoding** 'RENAME-OK))
                      (print-crlf/encoding** 'PERM-DENIED)))
                 ((ftp-dir-exists? path1)
                  (if (ftp-dir-allow-delete-move? path1 current-ftp-user)
                      (begin
                        (set! rename-path path1)
                        (print-crlf/encoding** 'RENAME-OK))
                      (print-crlf/encoding** 'PERM-DENIED)))
                 (else
                  (print-crlf/encoding** 'FILE-DIR-NOT-FOUND))))
              ((ftp-file-exists? path2)
               (if (ftp-file-allow-delete-move? path2 current-ftp-user)
                   (begin
                     (set! rename-path path2)
                     (print-crlf/encoding** 'RENAME-OK))
                   (print-crlf/encoding** 'PERM-DENIED)))
              ((ftp-dir-exists? path2)
               (if (ftp-dir-allow-delete-move? path2 current-ftp-user)
                   (begin
                     (set! rename-path path2)
                     (print-crlf/encoding** 'RENAME-OK))
                   (print-crlf/encoding** 'PERM-DENIED)))
              (else
               (print-crlf/encoding** 'FILE-DIR-NOT-FOUND))))
          (print-crlf/encoding** 'SYNTAX-ERROR "")))
    
    ;; Переименовывает подготавленный файл или каталог.
    (define (RNTO-COMMAND params)
      (local [(define (move file? old-path full-parent-path name user)
                (let ([new-path (string-append full-parent-path "/" name)])
                  (if (ftp-dir-allow-write? full-parent-path user)
                      (if (if file?
                              (ftp-file-exists? new-path)
                              (ftp-dir-exists? new-path))
                          (print-crlf/encoding** 'CANT-RENAME-EXIST)
                          (with-handlers ([exn:fail:filesystem?
                                           (λ (e) (print-crlf/encoding** 'CANT-RENAME))])
                            (when file?
                              (rename-file-or-directory (string-append old-path ".ftp-racket-file")
                                                        (string-append new-path ".ftp-racket-file")))
                            (rename-file-or-directory old-path new-path)
                            (print-log-event (format "Rename the file or directory from ~a to ~a"
                                                     (real-path->ftp-path old-path)
                                                     (real-path->ftp-path new-path)))
                            (print-crlf/encoding** 'CMD-SUCCESSFUL 250 "RNTO")))
                      (print-crlf/encoding** 'RENAME-PERM-DENIED))))]
        
        (if params
            (if rename-path
                (let* ([old-path rename-path]
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
                       [user current-ftp-user]
                       [curr-dir current-dir])
                  (cond
                    ((or (not name)
                         (and old-file? new-dir?))
                     (print-crlf/encoding** 'CANT-RENAME))
                    ((and (not parent-path)
                          (if old-file?
                              (ftp-file-exists? (string-append root-dir curr-dir "/" name))
                              (ftp-dir-exists? (string-append root-dir curr-dir "/" name))))
                     (print-crlf/encoding** 'CANT-RENAME-EXIST))
                    ((not parent-path)
                     (move old-file? old-path (string-append root-dir curr-dir) name user))
                    ((and (memq (string-ref parent-path 0) '(#\/ #\\))
                          (ftp-dir-exists? (string-append root-dir parent-path)))
                     (move old-file? old-path (string-append root-dir parent-path) name user))
                    ((ftp-dir-exists? (string-append root-dir curr-dir "/" parent-path))
                     (move old-file? old-path (string-append root-dir curr-dir "/" parent-path) name user))
                    (else
                     (print-crlf/encoding** 'CANT-RENAME)))
                  (set! rename-path #f))
                (print-crlf/encoding** 'RENAME-PERM-DENIED))
            (print-crlf/encoding** 'SYNTAX-ERROR ""))))
    
    ;; Инициирует пассивный режим.
    (define (PASV-COMMAND params)
      (if params
          (print-crlf/encoding** 'SYNTAX-ERROR "")
          (let* ([host-port passive-host-port]
                 [host (ftp-passive-host-port-host host-port)])
            (let-values ([(h5 h6) (quotient/remainder current-passive-port 256)])
              (print-crlf/encoding** 'PASV
                                     (bytes-ref host 0) (bytes-ref host 1) (bytes-ref host 2) (bytes-ref host 3)
                                     h5 h6))
            (set-ftp-passive-host-port-port! host-port current-passive-port)
            (set! DTP 'passive)
            (current-passive-port (if (>= current-passive-port passive-ports-to)
                                      passive-ports-from
                                      (add1 current-passive-port))))))
    
    ;; Experimental
    (define (HELP-COMMAND params)
      (if params
          (with-handlers ([any/c (λ (e) (print-crlf/encoding** 'UNKNOWN-CMD params))])
            (print-crlf/encoding** 'HELP (cdr (hash-ref cmd-voc (string-upcase params)))))
          (begin
            (print-crlf/encoding** 'HELP-LISTING)
            (for-each (λ (rec) (print-crlf/encoding* (format " ~a" (car rec)))) cmd-list)
            (print-crlf/encoding** 'END 214))))
    
    (define (ftp-data-transfer data [file? #f])
      (case DTP
        ((passive)
         (passive-data-transfer data file?))
        ((active)
         (active-data-transfer data file?))))
    
    ;; Experimental
    (define (active-data-transfer data file?)
      (let ([host (ftp-active-host-port-host active-host-port)]
            [port (ftp-active-host-port-port active-host-port)]
            [cust (make-custodian)])
        (set! current-process cust)
        (parameterize ([current-custodian cust])
          (thread (λ ()
                    (parameterize ([current-custodian cust])
                      (with-handlers ([any/c (λ (e)
                                               (print-crlf/encoding** 'TRANSFER-ABORTED))])
                        (let-values ([(in out) (tcp-connect (byte-host->string host) port)])
                          (print-crlf/encoding** 'OPEN-DATA-CONNECTION representation-type)
                          (if file?
                              (call-with-input-file data
                                (λ (in)
                                  (let loop ([dat (read-bytes 10048576 in)])
                                    (unless (eof-object? dat)
                                      (write-bytes dat out)
                                      (loop (read-bytes 10048576 in))))))
                              (case representation-type
                                ((ASCII)
                                 (print/locale-encoding out data))
                                ((Image)
                                 (write-bytes data out))))
                          (flush-output out)
                          (print-crlf/encoding** 'TRANSFER-OK)))
                      ;(displayln  "Process complete!" log-output-port)
                      (custodian-shutdown-all cust))))
          (thread (λ ()
                    (sleep 600)
                    ;(displayln "Auto kill working process!" log-output-port)
                    (custodian-shutdown-all cust))))))
    
    ;; Experimental
    (define (passive-data-transfer data file?)
      (let ([host (ftp-passive-host-port-host passive-host-port)]
            [port (ftp-passive-host-port-port passive-host-port)]
            [cust (make-custodian)])
        (set! current-process cust)
        (parameterize ([current-custodian cust])
          (thread (λ ()
                    (parameterize ([current-custodian cust])
                      (with-handlers ([any/c (λ (e)
                                               (print-crlf/encoding** 'TRANSFER-ABORTED))])
                        (let ([listener (get-passive-listener port)])
                          (let-values ([(in out) (tcp-accept listener)])
                            (print-crlf/encoding** 'OPEN-DATA-CONNECTION representation-type)
                            (if file?
                                (call-with-input-file data
                                  (λ (in)
                                    (let loop ([dat (read-bytes 10048576 in)])
                                      (unless (eof-object? dat)
                                        (write-bytes dat out)
                                        (loop (read-bytes 10048576 in))))))
                                (case representation-type
                                  ((ASCII)
                                   (print/locale-encoding out data))
                                  ((Image)
                                   (write-bytes data out))))
                            (flush-output out)
                            (print-crlf/encoding** 'TRANSFER-OK))))
                      ;(displayln "Process complete!" log-output-port)
                      (custodian-shutdown-all cust))))
          (thread (λ ()
                    (sleep 600)
                    ;(displayln "Auto kill working process!" log-output-port)
                    (custodian-shutdown-all cust))))))
    
    (define (ftp-store-file new-file-full-path exists-mode)
      (case DTP
        ((passive)
         (passive-store-file new-file-full-path exists-mode))
        ((active)
         (active-store-file new-file-full-path exists-mode))))
    
    ;; Experimental
    (define (active-store-file new-file-full-path exists-mode)
      (let ([host (ftp-active-host-port-host active-host-port)]
            [port (ftp-active-host-port-port active-host-port)]
            [cust (make-custodian)])
        (set! current-process cust)
        (parameterize ([current-custodian cust])
          (thread (λ ()
                    (with-handlers ([any/c (λ (e)
                                             (print-crlf/encoding** 'TRANSFER-ABORTED))])
                      (call-with-output-file new-file-full-path
                        (λ (fout)
                          (unless (file-exists? (string-append new-file-full-path ".ftp-racket-file"))
                            (ftp-mksys-file (string-append new-file-full-path ".ftp-racket-file")
                                            (ftp-user-login current-ftp-user) (ftp-user-group current-ftp-user)))
                          (parameterize ([current-custodian cust])
                            (let-values ([(in out) (tcp-connect (byte-host->string host) port)])
                              (print-crlf/encoding** 'OPEN-DATA-CONNECTION representation-type)
                              (when restart-marker
                                (file-position fout restart-marker)
                                (set! restart-marker #f))
                              (let loop ([dat (read-bytes 10048576 in)])
                                (unless (eof-object? dat)
                                  (write-bytes dat fout)
                                  (loop (read-bytes 10048576 in))))
                              (flush-output fout)
                              (print-log-event (format "~a data to file ~a"
                                                       (if (eq? exists-mode 'append) "Append" "Store")
                                                       (real-path->ftp-path new-file-full-path)))
                              (print-crlf/encoding** 'TRANSFER-OK))))
                        #:mode 'binary
                        #:exists exists-mode))
                    ;(displayln "Process complete!" log-output-port)
                    (custodian-shutdown-all cust)))
          (thread (λ ()
                    (sleep 600)
                    ;(displayln "Auto kill working process!" log-output-port)
                    (custodian-shutdown-all cust))))))
    
    ;; Experimental
    (define (passive-store-file new-file-full-path exists-mode)
      (let ([host (ftp-passive-host-port-host passive-host-port)]
            [port (ftp-passive-host-port-port passive-host-port)]
            [cust (make-custodian)])
        (set! current-process cust)
        (parameterize ([current-custodian cust])
          (thread (λ ()
                    (with-handlers ([any/c (λ (e)
                                             (print-crlf/encoding** 'TRANSFER-ABORTED))])
                      (call-with-output-file new-file-full-path
                        (λ (fout)
                          (unless (file-exists? (string-append new-file-full-path ".ftp-racket-file"))
                            (ftp-mksys-file (string-append new-file-full-path ".ftp-racket-file")
                                            (ftp-user-login current-ftp-user) (ftp-user-group current-ftp-user)))
                          (parameterize ([current-custodian cust])
                            (let ([listener (get-passive-listener port)])
                              (let-values ([(in out) (tcp-accept listener)])
                                (print-crlf/encoding** 'OPEN-DATA-CONNECTION representation-type)
                                (when restart-marker
                                  (file-position fout restart-marker)
                                  (set! restart-marker #f))
                                (let loop ([dat (read-bytes 10048576 in)])
                                  (unless (eof-object? dat)
                                    (write-bytes dat fout)
                                    (loop (read-bytes 10048576 in))))
                                (flush-output fout)
                                (print-log-event (format "~a data to file ~a"
                                                         (if (eq? exists-mode 'append) "Append" "Store")
                                                         (real-path->ftp-path new-file-full-path)))
                                (print-crlf/encoding** 'TRANSFER-OK)))))
                        #:mode 'binary
                        #:exists exists-mode))
                    ;(displayln "Process complete!" log-output-port)
                    (custodian-shutdown-all cust)))
          (thread (λ ()
                    (sleep 600)
                    ;(displayln "Auto kill working process!" log-output-port)
                    (custodian-shutdown-all cust))))))
    
    (define-syntax (passive-ports-from stx)
      #'(ftp-server-params-passive-ports-from server-params))
    
    (define-syntax (passive-ports-to stx)
      #'(ftp-server-params-passive-ports-to server-params))
    
    (define-syntax (passive-listeners stx)
      #'(ftp-server-params-passive-listeners server-params))
    
    (define-syntax (default-root-dir stx)
      #'(ftp-server-params-default-root-dir server-params))
    
    (define-syntax (default-locale-encoding stx)
      #'(ftp-server-params-default-locale-encoding server-params))
    
    (define-syntax (ftp-users stx)
      #'(ftp-server-params-ftp-users server-params))
    
    (define-syntax (log-output-port stx)
      #'(ftp-server-params-log-output-port server-params))
    
    (define-syntax (current-ftp-user stx)
      #'(hash-ref ftp-users user-id))
    
    (define-syntax (kill-current-ftp-process stx)
      #'(custodian-shutdown-all current-process))
    
    (define-syntax (current-passive-port stx)
      (syntax-case stx ()
        [(_ expr) #'(set-ftp-server-params-current-passive-port! server-params expr)]
        [_ #'(ftp-server-params-current-passive-port server-params)]))
    
    (define (get-passive-listener port)
      (vector-ref passive-listeners (- port passive-ports-from)))
    
    (define (byte-host->string byte-host)
      (string-join (map number->string (bytes->list byte-host))
                   "."))
    
    (define (read-request input-port)
      (if (byte-ready? input-port)
          (let ([line (read-bytes-line input-port)])
            (if (eof-object? line)
                line
                (let ([s (request-bytes->string/locale-encoding line)])
                  (substring s 0 (sub1 (string-length s))))))
          #f))
    
    (define (real-path->ftp-path real-path [drop-tail-elem 0])
      (simplify-ftp-path (substring real-path (string-length root-dir)) drop-tail-elem))
    
    (define (release-encoding-proc)
      (if (string=? locale-encoding "UTF-8")
          (begin
            (set! print/locale-encoding (λ (output-port text) (display text output-port)))
            (set! request-bytes->string/locale-encoding (λ (bstr) (bytes->string/utf-8 bstr)))
            (set! list-string->bytes/locale-encoding (λ (str) (string->bytes/utf-8 str))))
          (begin
            (set! print/locale-encoding (λ (output-port text) (print/encoding locale-encoding output-port text)))
            (set! request-bytes->string/locale-encoding (λ (bstr) (request-bytes->string/encoding locale-encoding bstr)))
            (set! list-string->bytes/locale-encoding (λ (str) (list-string->bytes/encoding locale-encoding str))))))
    
    (define (print-crlf/encoding* text)
      (print/locale-encoding client-output-port text)
      (write-bytes #"\r\n" client-output-port)
      (flush-output client-output-port))
    
    (define (print-crlf/encoding** response-tag . args)
      (let ([response (cdr (assq current-lang (hash-ref server-responses response-tag)))])
        (if (null? args)
            (print/locale-encoding client-output-port response)
            (print/locale-encoding client-output-port (apply format response args))))
      (write-bytes #"\r\n" client-output-port)
      (flush-output client-output-port))
    
    (define (print-log-event msg [user-name? #t])
      (if user-name?
          (fprintf log-output-port
                   "~a [~a] ~a : ~a\n" (date->string (current-date) #t) client-host user-id msg)
          (fprintf log-output-port
                   "~a [~a] ~a\n" (date->string (current-date) #t) client-host msg)))
    
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
             [path (string-append root-dir ftp-path)]
             [name (path->string (file-name-from-path ftp-path))]
             [file? (file-exists? path)]
             [info (ftp-file-or-dir-full-info (string-append path
                                                             (if file?
                                                                 ".ftp-racket-file"
                                                                 "/.ftp-racket-directory")))]
             [sysbytes (vector-ref info 0)]
             [user current-ftp-user]
             [user-name (ftp-user-login user)]
             [group (ftp-user-group user)]
             [features mlst-features])
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
    
    (define (init-cmd-voc)
      (set! cmd-list
            `(("ABOR" ,ABOR-COMMAND . "ABOR")
              ("ALLO" ,ALLO-COMMAND . "ALLO <SP> <decimal-integer>")
              ("APPE" ,(λ (params) (STORE-FILE params 'append)) . "APPE <SP> <pathname>")
              ("CDUP" ,CDUP-COMMAND . "CDUP")
              ("CLNT" ,CLNT-COMMAND . "CLNT <SP> <client-name>")
              ("CWD" ,CWD-COMMAND . "CWD <SP> <pathname>")
              ("DELE" ,DELE-COMMAND . "DELE <SP> <pathname>")
              ("FEAT" ,FEAT-COMMAND . "FEAT")
              ("HELP" ,HELP-COMMAND . "HELP [<SP> <string>]")
              ("LANG" ,LANG-COMMAND . "LANG <SP> <lang-tag>")
              ("LIST" ,(λ (params) (DIR-LIST params)) . "LIST [<SP> <pathname>]")
              ("MDTM" ,MDTM-COMMAND . "MDTM <SP> <pathname>")
              ("MKD" ,MKD-COMMAND . "MKD <SP> <pathname>")
              ("MLSD" ,MLSD-COMMAND . "MLSD [<SP> <pathname>]")
              ("MLST" ,MLST-COMMAND . "MLST [<SP> <pathname>]")
              ("MODE" ,MODE-COMMAND . "MODE <SP> <mode-code>")
              ("NLST" ,(λ (params) (DIR-LIST params #t)) . "NLST [<SP> <pathname>]")
              ("NOOP" ,NOOP-COMMAND . "NOOP")
              ("OPTS" ,OPTS-COMMAND . "OPTS <SP> <command-name> [<SP> <command-options>]")
              ("PASS" ,PASS-COMMAND . "PASS <SP> <password>")
              ("PASV" ,PASV-COMMAND . "PASV")
              ("PORT" ,PORT-COMMAND . "PORT <SP> <host-port>")
              ("PWD" ,PWD-COMMAND . "PWD")
              ("QUIT" ,QUIT-COMMAND . "QUIT")
              ("REIN" ,REIN-COMMAND . "REIN")
              ("REST" ,REST-COMMAND . "REST <SP> <marker>")
              ("RETR" ,RETR-COMMAND . "RETR <SP> <pathname>")
              ("RMD" ,RMD-COMMAND . "RMD <SP> <pathname>")
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
              ("XRMD" ,RMD-COMMAND . "XRMD <SP> <pathname>")))
      
      (set! cmd-voc (make-hash cmd-list))
      
      (set! server-responses
            (make-hash
             '((SYNTAX-ERROR (EN . "501 ~a Syntax error in parameters or arguments.")
                             (RU . "501 ~a Синтаксическая ошибка (неверный параметр или аргумент)."))
               (WELCOME (EN . "220 Racket FTP Server!")
                        (RU . "220 Racket FTP Сервер!"))
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
               (STATUS-INFO-1 (EN . "211-Racket FTP Server status:")
                              (RU . "211-Статус Racket FTP Сервера:"))
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
                     (RU . "227 Переход в пассивный режим (~a,~a,~a,~a,~a,~a)"))
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
               ))))
    
    (init-cmd-voc)
    (release-encoding-proc)
    (super-new)))

(define ftp-server%
  (class ftp-utils%
    (inherit get-params*
             ftp-dir-exists?
             ftp-mkdir*)
    ;;
    ;; ---------- Public Definitions ----------
    ;;
    ;;
    ;; ---------- Private Definitions ----------
    ;;
    (define server-params (default-server-params))
    (define server-custodian (make-parameter #f))
    ;;
    ;; ---------- Public Methods ----------
    ;;
    (define/public (set-passive-ports from to)
      (set-ftp-server-params-passive-ports-from! server-params from)
      (set-ftp-server-params-passive-ports-to! server-params to))
    
    (define/public (set-default-locale-encoding encoding)
      (set-ftp-server-params-default-locale-encoding! server-params encoding))
    
    (define/public (set-default-root-dir dir)
      (set-ftp-server-params-default-root-dir! server-params dir))
    
    (define/public (set-log-output-port output-port)
      (set-ftp-server-params-log-output-port! server-params output-port))
    
    (define/public (add-ftp-user full-name login pass group home-dirs [root-dir default-root-dir])
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
    
    (define/public (run [port 21] [max-allow-wait 50] [host "127.0.0.1"])
      (unless (server-custodian)
        (server-custodian (make-custodian))
        (init-ftp-dirs default-root-dir)
        (init-passive-listeners (server-custodian) host)
        (parameterize ([current-custodian (server-custodian)])
          (letrec ([listener (tcp-listen port max-allow-wait #t host)]
                   [main-loop (λ ()
                                (handle-client-request listener)
                                (main-loop))])
            (thread main-loop)))))
    
    (define/public (stop)
      (when (server-custodian)
        (custodian-shutdown-all (server-custodian))
        (server-custodian #f)))
    ;;
    ;; ---------- Private Methods ----------
    ;;
    (define/private (handle-client-request listener)
      (let ([cust (make-custodian)])
        (parameterize ([current-custodian cust])
          (let-values ([(in out) (tcp-accept listener)])
            (thread (λ ()
                      (let-values ([(server-host client-host) (tcp-addresses in)])
                        ;(fprintf log-output-port "[~a] Accept connection!\n" client-host)
                        (accept-client-request client-host in out (connect-shutdown 60 cust client-host))
                        ;(fprintf log-output-port "[~a] Connection close!\n" client-host)
                        )
                      (custodian-shutdown-all cust)))))))
    
    (define/private (connect-shutdown time connect-cust client-host)
      (parameterize ([current-custodian connect-cust])
        (let* ([tick time]
               [reset (λ () (set! tick time))])
          (thread (λ ()
                    (let loop ()
                      (when (> tick 0)
                        (sleep 1)
                        (set! tick (sub1 tick))
                        (loop)))
                    ;(fprintf log-output-port "[~a] Auto connection close!\n" client-host)
                    (custodian-shutdown-all connect-cust)))
          reset)))
    
    (define/private (accept-client-request host input-port output-port reset-timer)
      (with-handlers ([any/c #|displayln|# void])
        (send (new ftp-client% 
                   [client-host host]
                   [client-input-port input-port]
                   [client-output-port output-port]
                   [server-params server-params])
              eval-client-request reset-timer))
      (flush-output output-port))
    
    (define/private (init-ftp-dirs root-dir)
      (unless (ftp-dir-exists? root-dir)
        (ftp-mkdir* root-dir)))
    
    (define/private (init-passive-listeners server-custodian host)
      (parameterize ([current-custodian server-custodian])
        (passive-listeners (build-vector (- (add1 passive-ports-to) passive-ports-from)
                                         (λ (n) (tcp-listen (passive-ports-from . + . n) 1 #t host))))))
    
    (define-syntax (passive-listeners stx)
      (syntax-case stx ()
        [(_ expr) #'(set-ftp-server-params-passive-listeners! server-params expr)]
        [_ #'(ftp-server-params-passive-listeners server-params)]))
    
    (define-syntax (passive-ports-from stx)
      #'(ftp-server-params-passive-ports-from server-params))
    
    (define-syntax (passive-ports-to stx)
      #'(ftp-server-params-passive-ports-to server-params))
    
    (define-syntax (ftp-users stx)
      #'(ftp-server-params-ftp-users server-params))
    
    (define-syntax (default-root-dir stx)
      #'(ftp-server-params-default-root-dir server-params))
    
    (super-new)))