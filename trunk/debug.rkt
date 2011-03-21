#|

RFTPd Debug Library v1.0.0
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

(require (file "flags.rkt"))

(provide (all-defined-out))

(define-for-syntax (mcr:if-drdebug debug release)
  (if create-executable? release debug))

(define-syntax (if-drdebug so)
  (syntax-case so ()
    [(_ exp1 exp2)
     (mcr:if-drdebug #'exp1 #'exp2)]))

(define-syntax (debug/handler so)
  (mcr:if-drdebug (syntax-case so ()
                    [(_ info)
                     #'(lambda(e) (displayln info) (raise e))]
                    [_ #'raise])
                  #'void))