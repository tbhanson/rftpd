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

(provide (all-defined-out))

(define-syntax (and/exc stx)
  (syntax-case stx ()
    [(_ expr ...)
     #'(with-handlers ([any/c (λ (e) #f)])
         (and expr ...))]))

(define (string-split chlst str [start 0][end (string-length str)])
  (let loop [(i start)]
    (cond 
      [(= i end)
       (list (substring str start))]
      [(memq (string-ref str i) chlst)
       (cons (substring str start i) 
             (string-split chlst str (add1 i) end))]
      [else
       (loop (add1 i))])))

(define (string-split-char char str [start 0][end (string-length str)])
  (let loop [(i start)]
    (cond 
      [(= i end)
       (list (substring str start))]
      [(eq? (string-ref str i) char)
       (cons (substring str start i) 
             (string-split-char char str (add1 i) end))]
      [else
       (loop (add1 i))])))

(define (IPv4? ip)
  (and/exc (let ([l (string-split-char #\. ip)])
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

(define (private-IPv4? ip)
  (and/exc (let ([l (map string->number (string-split-char #\. ip))])
             (and ((length l) . = . 4)
                  (andmap byte? l)
                  (or ((first l). = . 10)
                      ((first l). = . 127)
                      (and ((first l). = . 169) 
                           ((second l). = . 254))
                      (and ((first l). = . 172)
                           ((second l). >= . 16)
                           ((second l). <= . 31))
                      (and ((first l). = . 192) 
                           ((second l). = . 168)))))))

(define (host-string? host)
  (and (string? host)
       (or (IPv4? host) (IPv6? host) (string-ci=? host))))

(define (port-number? port)
  (and (exact-nonnegative-integer? port) (port . <= . #xffff)))

(define (ssl-protocol? prt)
  (memq prt '(sslv2-or-v3 sslv2 sslv3 tls)))

(define (alarm-clock period count [event (λ() 1)])
  (let* ([tick count]
         [reset (λ () (set! tick count))]
         [thd (thread (λ ()
                        (do () [(<= tick 0) (event)]
                          (sleep period)
                          (set! tick (sub1 tick)))))]
         [kill (λ() (kill-thread thd))])
    (values reset kill)))