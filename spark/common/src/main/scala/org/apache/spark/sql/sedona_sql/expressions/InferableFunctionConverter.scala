/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.spark.sql.sedona_sql.expressions

import scala.reflect.runtime.universe.TypeTag

/**
 * Implicit conversions from Java/Scala functions to [[InferableFunction]]. This should be used in
 * conjunction with [[InferredExpression]] to make wrapping Java/Scala functions as catalyst
 * expressions much easier.
 */
object InferableFunctionConverter {
  // scalastyle:off line.size.limit
  implicit def inferableFunction1[R: InferableType, A1: InferableType](f: (A1) => R)(implicit
      typeTag: TypeTag[(A1) => R]): InferableFunction = InferableFunction(
    typeTag,
    argExtractors => {
      val func = f.asInstanceOf[(Any) => Any]
      val extractor1 = argExtractors(0)
      input => {
        val arg1 = extractor1(input)
        if (arg1 != null) {
          func(arg1)
        } else {
          null
        }
      }
    })

  implicit def inferableFunction2[R: InferableType, A1: InferableType, A2: InferableType](
      f: (A1, A2) => R)(implicit typeTag: TypeTag[(A1, A2) => R]): InferableFunction =
    InferableFunction(
      typeTag,
      argExtractors => {
        val func = f.asInstanceOf[(Any, Any) => Any]
        val extractor1 = argExtractors(0)
        val extractor2 = argExtractors(1)
        input => {
          val arg1 = extractor1(input)
          val arg2 = extractor2(input)
          if (arg1 != null && arg2 != null) {
            func(arg1, arg2)
          } else {
            null
          }
        }
      })

  implicit def inferableFunction3[
      R: InferableType,
      A1: InferableType,
      A2: InferableType,
      A3: InferableType](f: (A1, A2, A3) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3) => R]): InferableFunction = InferableFunction(
    typeTag,
    argExtractors => {
      val func = f.asInstanceOf[(Any, Any, Any) => Any]
      val extractor1 = argExtractors(0)
      val extractor2 = argExtractors(1)
      val extractor3 = argExtractors(2)
      input => {
        val arg1 = extractor1(input)
        val arg2 = extractor2(input)
        val arg3 = extractor3(input)
        if (arg1 != null && arg2 != null && arg3 != null) {
          func(arg1, arg2, arg3)
        } else {
          null
        }
      }
    })

  implicit def inferableFunction4[
      R: InferableType,
      A1: InferableType,
      A2: InferableType,
      A3: InferableType,
      A4: InferableType](f: (A1, A2, A3, A4) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4) => R]): InferableFunction = InferableFunction(
    typeTag,
    argExtractors => {
      val func = f.asInstanceOf[(Any, Any, Any, Any) => Any]
      val extractor1 = argExtractors(0)
      val extractor2 = argExtractors(1)
      val extractor3 = argExtractors(2)
      val extractor4 = argExtractors(3)
      input => {
        val arg1 = extractor1(input)
        val arg2 = extractor2(input)
        val arg3 = extractor3(input)
        val arg4 = extractor4(input)
        if (arg1 != null && arg2 != null && arg3 != null && arg4 != null) {
          func(arg1, arg2, arg3, arg4)
        } else {
          null
        }
      }
    })

  implicit def inferableFunction5[
      R: InferableType,
      A1: InferableType,
      A2: InferableType,
      A3: InferableType,
      A4: InferableType,
      A5: InferableType](f: (A1, A2, A3, A4, A5) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5) => R]): InferableFunction = InferableFunction(
    typeTag,
    argExtractors => {
      val func = f.asInstanceOf[(Any, Any, Any, Any, Any) => Any]
      val extractor1 = argExtractors(0)
      val extractor2 = argExtractors(1)
      val extractor3 = argExtractors(2)
      val extractor4 = argExtractors(3)
      val extractor5 = argExtractors(4)
      input => {
        val arg1 = extractor1(input)
        val arg2 = extractor2(input)
        val arg3 = extractor3(input)
        val arg4 = extractor4(input)
        val arg5 = extractor5(input)
        if (arg1 != null && arg2 != null && arg3 != null && arg4 != null && arg5 != null) {
          func(arg1, arg2, arg3, arg4, arg5)
        } else {
          null
        }
      }
    })

  implicit def inferableFunction6[
      R: InferableType,
      A1: InferableType,
      A2: InferableType,
      A3: InferableType,
      A4: InferableType,
      A5: InferableType,
      A6: InferableType](f: (A1, A2, A3, A4, A5, A6) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6) => R]): InferableFunction = InferableFunction(
    typeTag,
    argExtractors => {
      val func = f.asInstanceOf[(Any, Any, Any, Any, Any, Any) => Any]
      val extractor1 = argExtractors(0)
      val extractor2 = argExtractors(1)
      val extractor3 = argExtractors(2)
      val extractor4 = argExtractors(3)
      val extractor5 = argExtractors(4)
      val extractor6 = argExtractors(5)
      input => {
        val arg1 = extractor1(input)
        val arg2 = extractor2(input)
        val arg3 = extractor3(input)
        val arg4 = extractor4(input)
        val arg5 = extractor5(input)
        val arg6 = extractor6(input)
        if (arg1 != null && arg2 != null && arg3 != null && arg4 != null && arg5 != null && arg6 != null) {
          func(arg1, arg2, arg3, arg4, arg5, arg6)
        } else {
          null
        }
      }
    })

  implicit def inferableFunction7[
      R: InferableType,
      A1: InferableType,
      A2: InferableType,
      A3: InferableType,
      A4: InferableType,
      A5: InferableType,
      A6: InferableType,
      A7: InferableType](f: (A1, A2, A3, A4, A5, A6, A7) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7) => R]): InferableFunction =
    InferableFunction(
      typeTag,
      argExtractors => {
        val func = f.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any) => Any]
        val extractor1 = argExtractors(0)
        val extractor2 = argExtractors(1)
        val extractor3 = argExtractors(2)
        val extractor4 = argExtractors(3)
        val extractor5 = argExtractors(4)
        val extractor6 = argExtractors(5)
        val extractor7 = argExtractors(6)
        input => {
          val arg1 = extractor1(input)
          val arg2 = extractor2(input)
          val arg3 = extractor3(input)
          val arg4 = extractor4(input)
          val arg5 = extractor5(input)
          val arg6 = extractor6(input)
          val arg7 = extractor7(input)
          if (arg1 != null && arg2 != null && arg3 != null && arg4 != null && arg5 != null && arg6 != null && arg7 != null) {
            func(arg1, arg2, arg3, arg4, arg5, arg6, arg7)
          } else {
            null
          }
        }
      })

  implicit def inferableFunction8[
      R: InferableType,
      A1: InferableType,
      A2: InferableType,
      A3: InferableType,
      A4: InferableType,
      A5: InferableType,
      A6: InferableType,
      A7: InferableType,
      A8: InferableType](f: (A1, A2, A3, A4, A5, A6, A7, A8) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8) => R]): InferableFunction =
    InferableFunction(
      typeTag,
      argExtractors => {
        val func = f.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        val extractor1 = argExtractors(0)
        val extractor2 = argExtractors(1)
        val extractor3 = argExtractors(2)
        val extractor4 = argExtractors(3)
        val extractor5 = argExtractors(4)
        val extractor6 = argExtractors(5)
        val extractor7 = argExtractors(6)
        val extractor8 = argExtractors(7)
        input => {
          val arg1 = extractor1(input)
          val arg2 = extractor2(input)
          val arg3 = extractor3(input)
          val arg4 = extractor4(input)
          val arg5 = extractor5(input)
          val arg6 = extractor6(input)
          val arg7 = extractor7(input)
          val arg8 = extractor8(input)
          if (arg1 != null && arg2 != null && arg3 != null && arg4 != null && arg5 != null && arg6 != null && arg7 != null && arg8 != null) {
            func(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8)
          } else {
            null
          }
        }
      })

  implicit def inferableFunction9[
      R: InferableType,
      A1: InferableType,
      A2: InferableType,
      A3: InferableType,
      A4: InferableType,
      A5: InferableType,
      A6: InferableType,
      A7: InferableType,
      A8: InferableType,
      A9: InferableType](f: (A1, A2, A3, A4, A5, A6, A7, A8, A9) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9) => R]): InferableFunction =
    InferableFunction(
      typeTag,
      argExtractors => {
        val func = f.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        val extractor1 = argExtractors(0)
        val extractor2 = argExtractors(1)
        val extractor3 = argExtractors(2)
        val extractor4 = argExtractors(3)
        val extractor5 = argExtractors(4)
        val extractor6 = argExtractors(5)
        val extractor7 = argExtractors(6)
        val extractor8 = argExtractors(7)
        val extractor9 = argExtractors(8)
        input => {
          val arg1 = extractor1(input)
          val arg2 = extractor2(input)
          val arg3 = extractor3(input)
          val arg4 = extractor4(input)
          val arg5 = extractor5(input)
          val arg6 = extractor6(input)
          val arg7 = extractor7(input)
          val arg8 = extractor8(input)
          val arg9 = extractor9(input)
          if (arg1 != null && arg2 != null && arg3 != null && arg4 != null && arg5 != null && arg6 != null && arg7 != null && arg8 != null && arg9 != null) {
            func(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9)
          } else {
            null
          }
        }
      })

  implicit def inferableFunction10[
      R: InferableType,
      A1: InferableType,
      A2: InferableType,
      A3: InferableType,
      A4: InferableType,
      A5: InferableType,
      A6: InferableType,
      A7: InferableType,
      A8: InferableType,
      A9: InferableType,
      A10: InferableType](f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) => R]): InferableFunction =
    InferableFunction(
      typeTag,
      argExtractors => {
        val func = f.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        val extractor1 = argExtractors(0)
        val extractor2 = argExtractors(1)
        val extractor3 = argExtractors(2)
        val extractor4 = argExtractors(3)
        val extractor5 = argExtractors(4)
        val extractor6 = argExtractors(5)
        val extractor7 = argExtractors(6)
        val extractor8 = argExtractors(7)
        val extractor9 = argExtractors(8)
        val extractor10 = argExtractors(9)
        input => {
          val arg1 = extractor1(input)
          val arg2 = extractor2(input)
          val arg3 = extractor3(input)
          val arg4 = extractor4(input)
          val arg5 = extractor5(input)
          val arg6 = extractor6(input)
          val arg7 = extractor7(input)
          val arg8 = extractor8(input)
          val arg9 = extractor9(input)
          val arg10 = extractor10(input)
          if (arg1 != null && arg2 != null && arg3 != null && arg4 != null && arg5 != null && arg6 != null && arg7 != null && arg8 != null && arg9 != null && arg10 != null) {
            func(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10)
          } else {
            null
          }
        }
      })

  implicit def inferableFunction11[
      R: InferableType,
      A1: InferableType,
      A2: InferableType,
      A3: InferableType,
      A4: InferableType,
      A5: InferableType,
      A6: InferableType,
      A7: InferableType,
      A8: InferableType,
      A9: InferableType,
      A10: InferableType,
      A11: InferableType](f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11) => R]): InferableFunction =
    InferableFunction(
      typeTag,
      argExtractors => {
        val func = f.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        val extractor1 = argExtractors(0)
        val extractor2 = argExtractors(1)
        val extractor3 = argExtractors(2)
        val extractor4 = argExtractors(3)
        val extractor5 = argExtractors(4)
        val extractor6 = argExtractors(5)
        val extractor7 = argExtractors(6)
        val extractor8 = argExtractors(7)
        val extractor9 = argExtractors(8)
        val extractor10 = argExtractors(9)
        val extractor11 = argExtractors(10)
        input => {
          val arg1 = extractor1(input)
          val arg2 = extractor2(input)
          val arg3 = extractor3(input)
          val arg4 = extractor4(input)
          val arg5 = extractor5(input)
          val arg6 = extractor6(input)
          val arg7 = extractor7(input)
          val arg8 = extractor8(input)
          val arg9 = extractor9(input)
          val arg10 = extractor10(input)
          val arg11 = extractor11(input)
          if (arg1 != null && arg2 != null && arg3 != null && arg4 != null && arg5 != null && arg6 != null && arg7 != null && arg8 != null && arg9 != null && arg10 != null && arg11 != null) {
            func(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11)
          } else {
            null
          }
        }
      })

  implicit def inferableFunction12[
      R: InferableType,
      A1: InferableType,
      A2: InferableType,
      A3: InferableType,
      A4: InferableType,
      A5: InferableType,
      A6: InferableType,
      A7: InferableType,
      A8: InferableType,
      A9: InferableType,
      A10: InferableType,
      A11: InferableType,
      A12: InferableType](f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12) => R])
      : InferableFunction = InferableFunction(
    typeTag,
    argExtractors => {
      val func =
        f.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val extractor1 = argExtractors(0)
      val extractor2 = argExtractors(1)
      val extractor3 = argExtractors(2)
      val extractor4 = argExtractors(3)
      val extractor5 = argExtractors(4)
      val extractor6 = argExtractors(5)
      val extractor7 = argExtractors(6)
      val extractor8 = argExtractors(7)
      val extractor9 = argExtractors(8)
      val extractor10 = argExtractors(9)
      val extractor11 = argExtractors(10)
      val extractor12 = argExtractors(11)
      input => {
        val arg1 = extractor1(input)
        val arg2 = extractor2(input)
        val arg3 = extractor3(input)
        val arg4 = extractor4(input)
        val arg5 = extractor5(input)
        val arg6 = extractor6(input)
        val arg7 = extractor7(input)
        val arg8 = extractor8(input)
        val arg9 = extractor9(input)
        val arg10 = extractor10(input)
        val arg11 = extractor11(input)
        val arg12 = extractor12(input)
        if (arg1 != null && arg2 != null && arg3 != null && arg4 != null && arg5 != null && arg6 != null && arg7 != null && arg8 != null && arg9 != null && arg10 != null && arg11 != null && arg12 != null) {
          func(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12)
        } else {
          null
        }
      }
    })

  implicit def inferableFunction13[
      R: InferableType,
      A1: InferableType,
      A2: InferableType,
      A3: InferableType,
      A4: InferableType,
      A5: InferableType,
      A6: InferableType,
      A7: InferableType,
      A8: InferableType,
      A9: InferableType,
      A10: InferableType,
      A11: InferableType,
      A12: InferableType,
      A13: InferableType](f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13) => R)(
      implicit typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13) => R])
      : InferableFunction = InferableFunction(
    typeTag,
    argExtractors => {
      val func =
        f.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val extractor1 = argExtractors(0)
      val extractor2 = argExtractors(1)
      val extractor3 = argExtractors(2)
      val extractor4 = argExtractors(3)
      val extractor5 = argExtractors(4)
      val extractor6 = argExtractors(5)
      val extractor7 = argExtractors(6)
      val extractor8 = argExtractors(7)
      val extractor9 = argExtractors(8)
      val extractor10 = argExtractors(9)
      val extractor11 = argExtractors(10)
      val extractor12 = argExtractors(11)
      val extractor13 = argExtractors(12)
      input => {
        val arg1 = extractor1(input)
        val arg2 = extractor2(input)
        val arg3 = extractor3(input)
        val arg4 = extractor4(input)
        val arg5 = extractor5(input)
        val arg6 = extractor6(input)
        val arg7 = extractor7(input)
        val arg8 = extractor8(input)
        val arg9 = extractor9(input)
        val arg10 = extractor10(input)
        val arg11 = extractor11(input)
        val arg12 = extractor12(input)
        val arg13 = extractor13(input)
        if (arg1 != null && arg2 != null && arg3 != null && arg4 != null && arg5 != null && arg6 != null && arg7 != null && arg8 != null && arg9 != null && arg10 != null && arg11 != null && arg12 != null && arg13 != null) {
          func(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13)
        } else {
          null
        }
      }
    })

  implicit def inferableFunction14[
      R: InferableType,
      A1: InferableType,
      A2: InferableType,
      A3: InferableType,
      A4: InferableType,
      A5: InferableType,
      A6: InferableType,
      A7: InferableType,
      A8: InferableType,
      A9: InferableType,
      A10: InferableType,
      A11: InferableType,
      A12: InferableType,
      A13: InferableType,
      A14: InferableType](
      f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14) => R])
      : InferableFunction = InferableFunction(
    typeTag,
    argExtractors => {
      val func = f.asInstanceOf[
        (Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val extractor1 = argExtractors(0)
      val extractor2 = argExtractors(1)
      val extractor3 = argExtractors(2)
      val extractor4 = argExtractors(3)
      val extractor5 = argExtractors(4)
      val extractor6 = argExtractors(5)
      val extractor7 = argExtractors(6)
      val extractor8 = argExtractors(7)
      val extractor9 = argExtractors(8)
      val extractor10 = argExtractors(9)
      val extractor11 = argExtractors(10)
      val extractor12 = argExtractors(11)
      val extractor13 = argExtractors(12)
      val extractor14 = argExtractors(13)
      input => {
        val arg1 = extractor1(input)
        val arg2 = extractor2(input)
        val arg3 = extractor3(input)
        val arg4 = extractor4(input)
        val arg5 = extractor5(input)
        val arg6 = extractor6(input)
        val arg7 = extractor7(input)
        val arg8 = extractor8(input)
        val arg9 = extractor9(input)
        val arg10 = extractor10(input)
        val arg11 = extractor11(input)
        val arg12 = extractor12(input)
        val arg13 = extractor13(input)
        val arg14 = extractor14(input)
        if (arg1 != null && arg2 != null && arg3 != null && arg4 != null && arg5 != null && arg6 != null && arg7 != null && arg8 != null && arg9 != null && arg10 != null && arg11 != null && arg12 != null && arg13 != null && arg14 != null) {
          func(
            arg1,
            arg2,
            arg3,
            arg4,
            arg5,
            arg6,
            arg7,
            arg8,
            arg9,
            arg10,
            arg11,
            arg12,
            arg13,
            arg14)
        } else {
          null
        }
      }
    })

  implicit def inferableFunction15[
      R: InferableType,
      A1: InferableType,
      A2: InferableType,
      A3: InferableType,
      A4: InferableType,
      A5: InferableType,
      A6: InferableType,
      A7: InferableType,
      A8: InferableType,
      A9: InferableType,
      A10: InferableType,
      A11: InferableType,
      A12: InferableType,
      A13: InferableType,
      A14: InferableType,
      A15: InferableType](
      f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15) => R])
      : InferableFunction = InferableFunction(
    typeTag,
    argExtractors => {
      val func = f.asInstanceOf[
        (Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val extractor1 = argExtractors(0)
      val extractor2 = argExtractors(1)
      val extractor3 = argExtractors(2)
      val extractor4 = argExtractors(3)
      val extractor5 = argExtractors(4)
      val extractor6 = argExtractors(5)
      val extractor7 = argExtractors(6)
      val extractor8 = argExtractors(7)
      val extractor9 = argExtractors(8)
      val extractor10 = argExtractors(9)
      val extractor11 = argExtractors(10)
      val extractor12 = argExtractors(11)
      val extractor13 = argExtractors(12)
      val extractor14 = argExtractors(13)
      val extractor15 = argExtractors(14)
      input => {
        val arg1 = extractor1(input)
        val arg2 = extractor2(input)
        val arg3 = extractor3(input)
        val arg4 = extractor4(input)
        val arg5 = extractor5(input)
        val arg6 = extractor6(input)
        val arg7 = extractor7(input)
        val arg8 = extractor8(input)
        val arg9 = extractor9(input)
        val arg10 = extractor10(input)
        val arg11 = extractor11(input)
        val arg12 = extractor12(input)
        val arg13 = extractor13(input)
        val arg14 = extractor14(input)
        val arg15 = extractor15(input)
        if (arg1 != null && arg2 != null && arg3 != null && arg4 != null && arg5 != null && arg6 != null && arg7 != null && arg8 != null && arg9 != null && arg10 != null && arg11 != null && arg12 != null && arg13 != null && arg14 != null && arg15 != null) {
          func(
            arg1,
            arg2,
            arg3,
            arg4,
            arg5,
            arg6,
            arg7,
            arg8,
            arg9,
            arg10,
            arg11,
            arg12,
            arg13,
            arg14,
            arg15)
        } else {
          null
        }
      }
    })

  implicit def inferableFunction16[
      R: InferableType,
      A1: InferableType,
      A2: InferableType,
      A3: InferableType,
      A4: InferableType,
      A5: InferableType,
      A6: InferableType,
      A7: InferableType,
      A8: InferableType,
      A9: InferableType,
      A10: InferableType,
      A11: InferableType,
      A12: InferableType,
      A13: InferableType,
      A14: InferableType,
      A15: InferableType,
      A16: InferableType](
      f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16) => R)(implicit
  typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16) => R])
      : InferableFunction = InferableFunction(
    typeTag,
    argExtractors => {
      val func = f.asInstanceOf[
        (Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val extractor1 = argExtractors(0)
      val extractor2 = argExtractors(1)
      val extractor3 = argExtractors(2)
      val extractor4 = argExtractors(3)
      val extractor5 = argExtractors(4)
      val extractor6 = argExtractors(5)
      val extractor7 = argExtractors(6)
      val extractor8 = argExtractors(7)
      val extractor9 = argExtractors(8)
      val extractor10 = argExtractors(9)
      val extractor11 = argExtractors(10)
      val extractor12 = argExtractors(11)
      val extractor13 = argExtractors(12)
      val extractor14 = argExtractors(13)
      val extractor15 = argExtractors(14)
      val extractor16 = argExtractors(15)
      input => {
        val arg1 = extractor1(input)
        val arg2 = extractor2(input)
        val arg3 = extractor3(input)
        val arg4 = extractor4(input)
        val arg5 = extractor5(input)
        val arg6 = extractor6(input)
        val arg7 = extractor7(input)
        val arg8 = extractor8(input)
        val arg9 = extractor9(input)
        val arg10 = extractor10(input)
        val arg11 = extractor11(input)
        val arg12 = extractor12(input)
        val arg13 = extractor13(input)
        val arg14 = extractor14(input)
        val arg15 = extractor15(input)
        val arg16 = extractor16(input)
        if (arg1 != null && arg2 != null && arg3 != null && arg4 != null && arg5 != null && arg6 != null && arg7 != null && arg8 != null && arg9 != null && arg10 != null && arg11 != null && arg12 != null && arg13 != null && arg14 != null && arg15 != null && arg16 != null) {
          func(
            arg1,
            arg2,
            arg3,
            arg4,
            arg5,
            arg6,
            arg7,
            arg8,
            arg9,
            arg10,
            arg11,
            arg12,
            arg13,
            arg14,
            arg15,
            arg16)
        } else {
          null
        }
      }
    })

  // Here are the null tolerant variant of the above functions. User should use these functions if they want to tolerate
  // null values as function arguments. User needs to handle null arguments carefully in their function body when using
  // these functions.

  def nullTolerantInferableFunction1[R: InferableType, A1: InferableType](f: (A1) => R)(implicit
      typeTag: TypeTag[(A1) => R]): InferableFunction = InferableFunction(
    typeTag,
    argExtractors => {
      val func = f.asInstanceOf[(Any) => Any]
      val extractor1 = argExtractors(0)
      input => {
        val arg1 = extractor1(input)
        func(arg1)
      }
    })

  def nullTolerantInferableFunction2[R: InferableType, A1: InferableType, A2: InferableType](
      f: (A1, A2) => R)(implicit typeTag: TypeTag[(A1, A2) => R]): InferableFunction =
    InferableFunction(
      typeTag,
      argExtractors => {
        val func = f.asInstanceOf[(Any, Any) => Any]
        val extractor1 = argExtractors(0)
        val extractor2 = argExtractors(1)
        input => {
          val arg1 = extractor1(input)
          val arg2 = extractor2(input)
          func(arg1, arg2)
        }
      })

  def nullTolerantInferableFunction3[
      R: InferableType,
      A1: InferableType,
      A2: InferableType,
      A3: InferableType](f: (A1, A2, A3) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3) => R]): InferableFunction = InferableFunction(
    typeTag,
    argExtractors => {
      val func = f.asInstanceOf[(Any, Any, Any) => Any]
      val extractor1 = argExtractors(0)
      val extractor2 = argExtractors(1)
      val extractor3 = argExtractors(2)
      input => {
        val arg1 = extractor1(input)
        val arg2 = extractor2(input)
        val arg3 = extractor3(input)
        func(arg1, arg2, arg3)
      }
    })

  def nullTolerantInferableFunction4[
      R: InferableType,
      A1: InferableType,
      A2: InferableType,
      A3: InferableType,
      A4: InferableType](f: (A1, A2, A3, A4) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4) => R]): InferableFunction = InferableFunction(
    typeTag,
    argExtractors => {
      val func = f.asInstanceOf[(Any, Any, Any, Any) => Any]
      val extractor1 = argExtractors(0)
      val extractor2 = argExtractors(1)
      val extractor3 = argExtractors(2)
      val extractor4 = argExtractors(3)
      input => {
        val arg1 = extractor1(input)
        val arg2 = extractor2(input)
        val arg3 = extractor3(input)
        val arg4 = extractor4(input)
        func(arg1, arg2, arg3, arg4)
      }
    })

  def nullTolerantInferableFunction5[
      R: InferableType,
      A1: InferableType,
      A2: InferableType,
      A3: InferableType,
      A4: InferableType,
      A5: InferableType](f: (A1, A2, A3, A4, A5) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5) => R]): InferableFunction = InferableFunction(
    typeTag,
    argExtractors => {
      val func = f.asInstanceOf[(Any, Any, Any, Any, Any) => Any]
      val extractor1 = argExtractors(0)
      val extractor2 = argExtractors(1)
      val extractor3 = argExtractors(2)
      val extractor4 = argExtractors(3)
      val extractor5 = argExtractors(4)
      input => {
        val arg1 = extractor1(input)
        val arg2 = extractor2(input)
        val arg3 = extractor3(input)
        val arg4 = extractor4(input)
        val arg5 = extractor5(input)
        func(arg1, arg2, arg3, arg4, arg5)
      }
    })

  def nullTolerantInferableFunction6[
      R: InferableType,
      A1: InferableType,
      A2: InferableType,
      A3: InferableType,
      A4: InferableType,
      A5: InferableType,
      A6: InferableType](f: (A1, A2, A3, A4, A5, A6) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6) => R]): InferableFunction = InferableFunction(
    typeTag,
    argExtractors => {
      val func = f.asInstanceOf[(Any, Any, Any, Any, Any, Any) => Any]
      val extractor1 = argExtractors(0)
      val extractor2 = argExtractors(1)
      val extractor3 = argExtractors(2)
      val extractor4 = argExtractors(3)
      val extractor5 = argExtractors(4)
      val extractor6 = argExtractors(5)
      input => {
        val arg1 = extractor1(input)
        val arg2 = extractor2(input)
        val arg3 = extractor3(input)
        val arg4 = extractor4(input)
        val arg5 = extractor5(input)
        val arg6 = extractor6(input)
        func(arg1, arg2, arg3, arg4, arg5, arg6)
      }
    })

  def nullTolerantInferableFunction7[
      R: InferableType,
      A1: InferableType,
      A2: InferableType,
      A3: InferableType,
      A4: InferableType,
      A5: InferableType,
      A6: InferableType,
      A7: InferableType](f: (A1, A2, A3, A4, A5, A6, A7) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7) => R]): InferableFunction =
    InferableFunction(
      typeTag,
      argExtractors => {
        val func = f.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any) => Any]
        val extractor1 = argExtractors(0)
        val extractor2 = argExtractors(1)
        val extractor3 = argExtractors(2)
        val extractor4 = argExtractors(3)
        val extractor5 = argExtractors(4)
        val extractor6 = argExtractors(5)
        val extractor7 = argExtractors(6)
        input => {
          val arg1 = extractor1(input)
          val arg2 = extractor2(input)
          val arg3 = extractor3(input)
          val arg4 = extractor4(input)
          val arg5 = extractor5(input)
          val arg6 = extractor6(input)
          val arg7 = extractor7(input)
          func(arg1, arg2, arg3, arg4, arg5, arg6, arg7)
        }
      })

  def nullTolerantInferableFunction8[
      R: InferableType,
      A1: InferableType,
      A2: InferableType,
      A3: InferableType,
      A4: InferableType,
      A5: InferableType,
      A6: InferableType,
      A7: InferableType,
      A8: InferableType](f: (A1, A2, A3, A4, A5, A6, A7, A8) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8) => R]): InferableFunction =
    InferableFunction(
      typeTag,
      argExtractors => {
        val func = f.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        val extractor1 = argExtractors(0)
        val extractor2 = argExtractors(1)
        val extractor3 = argExtractors(2)
        val extractor4 = argExtractors(3)
        val extractor5 = argExtractors(4)
        val extractor6 = argExtractors(5)
        val extractor7 = argExtractors(6)
        val extractor8 = argExtractors(7)
        input => {
          val arg1 = extractor1(input)
          val arg2 = extractor2(input)
          val arg3 = extractor3(input)
          val arg4 = extractor4(input)
          val arg5 = extractor5(input)
          val arg6 = extractor6(input)
          val arg7 = extractor7(input)
          val arg8 = extractor8(input)
          func(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8)
        }
      })

  def nullTolerantInferableFunction9[
      R: InferableType,
      A1: InferableType,
      A2: InferableType,
      A3: InferableType,
      A4: InferableType,
      A5: InferableType,
      A6: InferableType,
      A7: InferableType,
      A8: InferableType,
      A9: InferableType](f: (A1, A2, A3, A4, A5, A6, A7, A8, A9) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9) => R]): InferableFunction =
    InferableFunction(
      typeTag,
      argExtractors => {
        val func = f.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        val extractor1 = argExtractors(0)
        val extractor2 = argExtractors(1)
        val extractor3 = argExtractors(2)
        val extractor4 = argExtractors(3)
        val extractor5 = argExtractors(4)
        val extractor6 = argExtractors(5)
        val extractor7 = argExtractors(6)
        val extractor8 = argExtractors(7)
        val extractor9 = argExtractors(8)
        input => {
          val arg1 = extractor1(input)
          val arg2 = extractor2(input)
          val arg3 = extractor3(input)
          val arg4 = extractor4(input)
          val arg5 = extractor5(input)
          val arg6 = extractor6(input)
          val arg7 = extractor7(input)
          val arg8 = extractor8(input)
          val arg9 = extractor9(input)
          func(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9)
        }
      })

  def nullTolerantInferableFunction10[
      R: InferableType,
      A1: InferableType,
      A2: InferableType,
      A3: InferableType,
      A4: InferableType,
      A5: InferableType,
      A6: InferableType,
      A7: InferableType,
      A8: InferableType,
      A9: InferableType,
      A10: InferableType](f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) => R]): InferableFunction =
    InferableFunction(
      typeTag,
      argExtractors => {
        val func = f.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        val extractor1 = argExtractors(0)
        val extractor2 = argExtractors(1)
        val extractor3 = argExtractors(2)
        val extractor4 = argExtractors(3)
        val extractor5 = argExtractors(4)
        val extractor6 = argExtractors(5)
        val extractor7 = argExtractors(6)
        val extractor8 = argExtractors(7)
        val extractor9 = argExtractors(8)
        val extractor10 = argExtractors(9)
        input => {
          val arg1 = extractor1(input)
          val arg2 = extractor2(input)
          val arg3 = extractor3(input)
          val arg4 = extractor4(input)
          val arg5 = extractor5(input)
          val arg6 = extractor6(input)
          val arg7 = extractor7(input)
          val arg8 = extractor8(input)
          val arg9 = extractor9(input)
          val arg10 = extractor10(input)
          func(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10)
        }
      })

  def nullTolerantInferableFunction11[
      R: InferableType,
      A1: InferableType,
      A2: InferableType,
      A3: InferableType,
      A4: InferableType,
      A5: InferableType,
      A6: InferableType,
      A7: InferableType,
      A8: InferableType,
      A9: InferableType,
      A10: InferableType,
      A11: InferableType](f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11) => R]): InferableFunction =
    InferableFunction(
      typeTag,
      argExtractors => {
        val func = f.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        val extractor1 = argExtractors(0)
        val extractor2 = argExtractors(1)
        val extractor3 = argExtractors(2)
        val extractor4 = argExtractors(3)
        val extractor5 = argExtractors(4)
        val extractor6 = argExtractors(5)
        val extractor7 = argExtractors(6)
        val extractor8 = argExtractors(7)
        val extractor9 = argExtractors(8)
        val extractor10 = argExtractors(9)
        val extractor11 = argExtractors(10)
        input => {
          val arg1 = extractor1(input)
          val arg2 = extractor2(input)
          val arg3 = extractor3(input)
          val arg4 = extractor4(input)
          val arg5 = extractor5(input)
          val arg6 = extractor6(input)
          val arg7 = extractor7(input)
          val arg8 = extractor8(input)
          val arg9 = extractor9(input)
          val arg10 = extractor10(input)
          val arg11 = extractor11(input)
          func(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11)
        }
      })

  def nullTolerantInferableFunction12[
      R: InferableType,
      A1: InferableType,
      A2: InferableType,
      A3: InferableType,
      A4: InferableType,
      A5: InferableType,
      A6: InferableType,
      A7: InferableType,
      A8: InferableType,
      A9: InferableType,
      A10: InferableType,
      A11: InferableType,
      A12: InferableType](f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12) => R])
      : InferableFunction = InferableFunction(
    typeTag,
    argExtractors => {
      val func =
        f.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val extractor1 = argExtractors(0)
      val extractor2 = argExtractors(1)
      val extractor3 = argExtractors(2)
      val extractor4 = argExtractors(3)
      val extractor5 = argExtractors(4)
      val extractor6 = argExtractors(5)
      val extractor7 = argExtractors(6)
      val extractor8 = argExtractors(7)
      val extractor9 = argExtractors(8)
      val extractor10 = argExtractors(9)
      val extractor11 = argExtractors(10)
      val extractor12 = argExtractors(11)
      input => {
        val arg1 = extractor1(input)
        val arg2 = extractor2(input)
        val arg3 = extractor3(input)
        val arg4 = extractor4(input)
        val arg5 = extractor5(input)
        val arg6 = extractor6(input)
        val arg7 = extractor7(input)
        val arg8 = extractor8(input)
        val arg9 = extractor9(input)
        val arg10 = extractor10(input)
        val arg11 = extractor11(input)
        val arg12 = extractor12(input)
        func(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12)
      }
    })

  def nullTolerantInferableFunction13[
      R: InferableType,
      A1: InferableType,
      A2: InferableType,
      A3: InferableType,
      A4: InferableType,
      A5: InferableType,
      A6: InferableType,
      A7: InferableType,
      A8: InferableType,
      A9: InferableType,
      A10: InferableType,
      A11: InferableType,
      A12: InferableType,
      A13: InferableType](f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13) => R)(
      implicit typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13) => R])
      : InferableFunction = InferableFunction(
    typeTag,
    argExtractors => {
      val func =
        f.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val extractor1 = argExtractors(0)
      val extractor2 = argExtractors(1)
      val extractor3 = argExtractors(2)
      val extractor4 = argExtractors(3)
      val extractor5 = argExtractors(4)
      val extractor6 = argExtractors(5)
      val extractor7 = argExtractors(6)
      val extractor8 = argExtractors(7)
      val extractor9 = argExtractors(8)
      val extractor10 = argExtractors(9)
      val extractor11 = argExtractors(10)
      val extractor12 = argExtractors(11)
      val extractor13 = argExtractors(12)
      input => {
        val arg1 = extractor1(input)
        val arg2 = extractor2(input)
        val arg3 = extractor3(input)
        val arg4 = extractor4(input)
        val arg5 = extractor5(input)
        val arg6 = extractor6(input)
        val arg7 = extractor7(input)
        val arg8 = extractor8(input)
        val arg9 = extractor9(input)
        val arg10 = extractor10(input)
        val arg11 = extractor11(input)
        val arg12 = extractor12(input)
        val arg13 = extractor13(input)
        func(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13)
      }
    })

  def nullTolerantInferableFunction14[
      R: InferableType,
      A1: InferableType,
      A2: InferableType,
      A3: InferableType,
      A4: InferableType,
      A5: InferableType,
      A6: InferableType,
      A7: InferableType,
      A8: InferableType,
      A9: InferableType,
      A10: InferableType,
      A11: InferableType,
      A12: InferableType,
      A13: InferableType,
      A14: InferableType](
      f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14) => R])
      : InferableFunction = InferableFunction(
    typeTag,
    argExtractors => {
      val func = f.asInstanceOf[
        (Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val extractor1 = argExtractors(0)
      val extractor2 = argExtractors(1)
      val extractor3 = argExtractors(2)
      val extractor4 = argExtractors(3)
      val extractor5 = argExtractors(4)
      val extractor6 = argExtractors(5)
      val extractor7 = argExtractors(6)
      val extractor8 = argExtractors(7)
      val extractor9 = argExtractors(8)
      val extractor10 = argExtractors(9)
      val extractor11 = argExtractors(10)
      val extractor12 = argExtractors(11)
      val extractor13 = argExtractors(12)
      val extractor14 = argExtractors(13)
      input => {
        val arg1 = extractor1(input)
        val arg2 = extractor2(input)
        val arg3 = extractor3(input)
        val arg4 = extractor4(input)
        val arg5 = extractor5(input)
        val arg6 = extractor6(input)
        val arg7 = extractor7(input)
        val arg8 = extractor8(input)
        val arg9 = extractor9(input)
        val arg10 = extractor10(input)
        val arg11 = extractor11(input)
        val arg12 = extractor12(input)
        val arg13 = extractor13(input)
        val arg14 = extractor14(input)
        func(
          arg1,
          arg2,
          arg3,
          arg4,
          arg5,
          arg6,
          arg7,
          arg8,
          arg9,
          arg10,
          arg11,
          arg12,
          arg13,
          arg14)
      }
    })

  def nullTolerantInferableFunction15[
      R: InferableType,
      A1: InferableType,
      A2: InferableType,
      A3: InferableType,
      A4: InferableType,
      A5: InferableType,
      A6: InferableType,
      A7: InferableType,
      A8: InferableType,
      A9: InferableType,
      A10: InferableType,
      A11: InferableType,
      A12: InferableType,
      A13: InferableType,
      A14: InferableType,
      A15: InferableType](
      f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15) => R])
      : InferableFunction = InferableFunction(
    typeTag,
    argExtractors => {
      val func = f.asInstanceOf[
        (Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val extractor1 = argExtractors(0)
      val extractor2 = argExtractors(1)
      val extractor3 = argExtractors(2)
      val extractor4 = argExtractors(3)
      val extractor5 = argExtractors(4)
      val extractor6 = argExtractors(5)
      val extractor7 = argExtractors(6)
      val extractor8 = argExtractors(7)
      val extractor9 = argExtractors(8)
      val extractor10 = argExtractors(9)
      val extractor11 = argExtractors(10)
      val extractor12 = argExtractors(11)
      val extractor13 = argExtractors(12)
      val extractor14 = argExtractors(13)
      val extractor15 = argExtractors(14)
      input => {
        val arg1 = extractor1(input)
        val arg2 = extractor2(input)
        val arg3 = extractor3(input)
        val arg4 = extractor4(input)
        val arg5 = extractor5(input)
        val arg6 = extractor6(input)
        val arg7 = extractor7(input)
        val arg8 = extractor8(input)
        val arg9 = extractor9(input)
        val arg10 = extractor10(input)
        val arg11 = extractor11(input)
        val arg12 = extractor12(input)
        val arg13 = extractor13(input)
        val arg14 = extractor14(input)
        val arg15 = extractor15(input)
        func(
          arg1,
          arg2,
          arg3,
          arg4,
          arg5,
          arg6,
          arg7,
          arg8,
          arg9,
          arg10,
          arg11,
          arg12,
          arg13,
          arg14,
          arg15)
      }
    })

  def nullTolerantInferableFunction16[
      R: InferableType,
      A1: InferableType,
      A2: InferableType,
      A3: InferableType,
      A4: InferableType,
      A5: InferableType,
      A6: InferableType,
      A7: InferableType,
      A8: InferableType,
      A9: InferableType,
      A10: InferableType,
      A11: InferableType,
      A12: InferableType,
      A13: InferableType,
      A14: InferableType,
      A15: InferableType,
      A16: InferableType](
      f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16) => R)(implicit
  typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16) => R])
      : InferableFunction = InferableFunction(
    typeTag,
    argExtractors => {
      val func = f.asInstanceOf[
        (Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val extractor1 = argExtractors(0)
      val extractor2 = argExtractors(1)
      val extractor3 = argExtractors(2)
      val extractor4 = argExtractors(3)
      val extractor5 = argExtractors(4)
      val extractor6 = argExtractors(5)
      val extractor7 = argExtractors(6)
      val extractor8 = argExtractors(7)
      val extractor9 = argExtractors(8)
      val extractor10 = argExtractors(9)
      val extractor11 = argExtractors(10)
      val extractor12 = argExtractors(11)
      val extractor13 = argExtractors(12)
      val extractor14 = argExtractors(13)
      val extractor15 = argExtractors(14)
      val extractor16 = argExtractors(15)
      input => {
        val arg1 = extractor1(input)
        val arg2 = extractor2(input)
        val arg3 = extractor3(input)
        val arg4 = extractor4(input)
        val arg5 = extractor5(input)
        val arg6 = extractor6(input)
        val arg7 = extractor7(input)
        val arg8 = extractor8(input)
        val arg9 = extractor9(input)
        val arg10 = extractor10(input)
        val arg11 = extractor11(input)
        val arg12 = extractor12(input)
        val arg13 = extractor13(input)
        val arg14 = extractor14(input)
        val arg15 = extractor15(input)
        val arg16 = extractor16(input)
        func(
          arg1,
          arg2,
          arg3,
          arg4,
          arg5,
          arg6,
          arg7,
          arg8,
          arg9,
          arg10,
          arg11,
          arg12,
          arg13,
          arg14,
          arg15,
          arg16)
      }
    })
  // scalastyle:on
}
