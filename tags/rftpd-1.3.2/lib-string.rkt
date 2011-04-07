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

(define (delete-lws str [ws '(#\space #\tab)])
  (do [(i 0 (add1 i))]
    [(not (memq (string-ref str i) ws))
     (substring str i)]))

(define (delete-rws str [ws '(#\space #\tab)]);error!
  (do [(i (string-length str) (sub1 i))]
    [(not (memq (string-ref str (sub1 i)) ws))
     (substring str 0 i)]))

(define (delete-lrws str [ws '(#\space #\tab)])
  (let ([start 0]
        [end (string-length str)])
    (do [] [(not (memq (string-ref str start) ws))]
      (set! start (add1 start)))
    (do [] [(not (memq (string-ref str (sub1 end)) ws))]
      (set! end (sub1 end)))
    (substring str start end)))