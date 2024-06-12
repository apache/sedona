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
 * Implicit conversions from Java/Scala functions to [[InferrableFunction]]. This should be used
 * in conjunction with [[InferredExpression]] to make wrapping Java/Scala functions as catalyst
 * expressions much easier.
 */
object InferrableFunctionConverter {
  // scalastyle:off line.size.limit
  implicit def inferrableFunction1[R: InferrableType, A1: InferrableType](f: (A1) => R)(implicit
      typeTag: TypeTag[(A1) => R]): InferrableFunction = InferrableFunction(
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

  implicit def inferrableFunction2[R: InferrableType, A1: InferrableType, A2: InferrableType](
      f: (A1, A2) => R)(implicit typeTag: TypeTag[(A1, A2) => R]): InferrableFunction =
    InferrableFunction(
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

  implicit def inferrableFunction3[
      R: InferrableType,
      A1: InferrableType,
      A2: InferrableType,
      A3: InferrableType](f: (A1, A2, A3) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3) => R]): InferrableFunction = InferrableFunction(
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

  implicit def inferrableFunction4[
      R: InferrableType,
      A1: InferrableType,
      A2: InferrableType,
      A3: InferrableType,
      A4: InferrableType](f: (A1, A2, A3, A4) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4) => R]): InferrableFunction = InferrableFunction(
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

  implicit def inferrableFunction5[
      R: InferrableType,
      A1: InferrableType,
      A2: InferrableType,
      A3: InferrableType,
      A4: InferrableType,
      A5: InferrableType](f: (A1, A2, A3, A4, A5) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5) => R]): InferrableFunction = InferrableFunction(
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

  implicit def inferrableFunction6[
      R: InferrableType,
      A1: InferrableType,
      A2: InferrableType,
      A3: InferrableType,
      A4: InferrableType,
      A5: InferrableType,
      A6: InferrableType](f: (A1, A2, A3, A4, A5, A6) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6) => R]): InferrableFunction = InferrableFunction(
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

  implicit def inferrableFunction7[
      R: InferrableType,
      A1: InferrableType,
      A2: InferrableType,
      A3: InferrableType,
      A4: InferrableType,
      A5: InferrableType,
      A6: InferrableType,
      A7: InferrableType](f: (A1, A2, A3, A4, A5, A6, A7) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7) => R]): InferrableFunction =
    InferrableFunction(
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

  implicit def inferrableFunction8[
      R: InferrableType,
      A1: InferrableType,
      A2: InferrableType,
      A3: InferrableType,
      A4: InferrableType,
      A5: InferrableType,
      A6: InferrableType,
      A7: InferrableType,
      A8: InferrableType](f: (A1, A2, A3, A4, A5, A6, A7, A8) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8) => R]): InferrableFunction =
    InferrableFunction(
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

  implicit def inferrableFunction9[
      R: InferrableType,
      A1: InferrableType,
      A2: InferrableType,
      A3: InferrableType,
      A4: InferrableType,
      A5: InferrableType,
      A6: InferrableType,
      A7: InferrableType,
      A8: InferrableType,
      A9: InferrableType](f: (A1, A2, A3, A4, A5, A6, A7, A8, A9) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9) => R]): InferrableFunction =
    InferrableFunction(
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

  implicit def inferrableFunction10[
      R: InferrableType,
      A1: InferrableType,
      A2: InferrableType,
      A3: InferrableType,
      A4: InferrableType,
      A5: InferrableType,
      A6: InferrableType,
      A7: InferrableType,
      A8: InferrableType,
      A9: InferrableType,
      A10: InferrableType](f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) => R]): InferrableFunction =
    InferrableFunction(
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

  implicit def inferrableFunction11[
      R: InferrableType,
      A1: InferrableType,
      A2: InferrableType,
      A3: InferrableType,
      A4: InferrableType,
      A5: InferrableType,
      A6: InferrableType,
      A7: InferrableType,
      A8: InferrableType,
      A9: InferrableType,
      A10: InferrableType,
      A11: InferrableType](f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11) => R]): InferrableFunction =
    InferrableFunction(
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

  implicit def inferrableFunction12[
      R: InferrableType,
      A1: InferrableType,
      A2: InferrableType,
      A3: InferrableType,
      A4: InferrableType,
      A5: InferrableType,
      A6: InferrableType,
      A7: InferrableType,
      A8: InferrableType,
      A9: InferrableType,
      A10: InferrableType,
      A11: InferrableType,
      A12: InferrableType](f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12) => R])
      : InferrableFunction = InferrableFunction(
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

  implicit def inferrableFunction13[
      R: InferrableType,
      A1: InferrableType,
      A2: InferrableType,
      A3: InferrableType,
      A4: InferrableType,
      A5: InferrableType,
      A6: InferrableType,
      A7: InferrableType,
      A8: InferrableType,
      A9: InferrableType,
      A10: InferrableType,
      A11: InferrableType,
      A12: InferrableType,
      A13: InferrableType](f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13) => R)(
      implicit typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13) => R])
      : InferrableFunction = InferrableFunction(
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

  implicit def inferrableFunction14[
      R: InferrableType,
      A1: InferrableType,
      A2: InferrableType,
      A3: InferrableType,
      A4: InferrableType,
      A5: InferrableType,
      A6: InferrableType,
      A7: InferrableType,
      A8: InferrableType,
      A9: InferrableType,
      A10: InferrableType,
      A11: InferrableType,
      A12: InferrableType,
      A13: InferrableType,
      A14: InferrableType](
      f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14) => R])
      : InferrableFunction = InferrableFunction(
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

  implicit def inferrableFunction15[
      R: InferrableType,
      A1: InferrableType,
      A2: InferrableType,
      A3: InferrableType,
      A4: InferrableType,
      A5: InferrableType,
      A6: InferrableType,
      A7: InferrableType,
      A8: InferrableType,
      A9: InferrableType,
      A10: InferrableType,
      A11: InferrableType,
      A12: InferrableType,
      A13: InferrableType,
      A14: InferrableType,
      A15: InferrableType](
      f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15) => R])
      : InferrableFunction = InferrableFunction(
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

  implicit def inferrableFunction16[
      R: InferrableType,
      A1: InferrableType,
      A2: InferrableType,
      A3: InferrableType,
      A4: InferrableType,
      A5: InferrableType,
      A6: InferrableType,
      A7: InferrableType,
      A8: InferrableType,
      A9: InferrableType,
      A10: InferrableType,
      A11: InferrableType,
      A12: InferrableType,
      A13: InferrableType,
      A14: InferrableType,
      A15: InferrableType,
      A16: InferrableType](
      f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16) => R)(implicit
  typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16) => R])
      : InferrableFunction = InferrableFunction(
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

  def nullTolerantInferrableFunction1[R: InferrableType, A1: InferrableType](f: (A1) => R)(
      implicit typeTag: TypeTag[(A1) => R]): InferrableFunction = InferrableFunction(
    typeTag,
    argExtractors => {
      val func = f.asInstanceOf[(Any) => Any]
      val extractor1 = argExtractors(0)
      input => {
        val arg1 = extractor1(input)
        func(arg1)
      }
    })

  def nullTolerantInferrableFunction2[R: InferrableType, A1: InferrableType, A2: InferrableType](
      f: (A1, A2) => R)(implicit typeTag: TypeTag[(A1, A2) => R]): InferrableFunction =
    InferrableFunction(
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

  def nullTolerantInferrableFunction3[
      R: InferrableType,
      A1: InferrableType,
      A2: InferrableType,
      A3: InferrableType](f: (A1, A2, A3) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3) => R]): InferrableFunction = InferrableFunction(
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

  def nullTolerantInferrableFunction4[
      R: InferrableType,
      A1: InferrableType,
      A2: InferrableType,
      A3: InferrableType,
      A4: InferrableType](f: (A1, A2, A3, A4) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4) => R]): InferrableFunction = InferrableFunction(
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

  def nullTolerantInferrableFunction5[
      R: InferrableType,
      A1: InferrableType,
      A2: InferrableType,
      A3: InferrableType,
      A4: InferrableType,
      A5: InferrableType](f: (A1, A2, A3, A4, A5) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5) => R]): InferrableFunction = InferrableFunction(
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

  def nullTolerantInferrableFunction6[
      R: InferrableType,
      A1: InferrableType,
      A2: InferrableType,
      A3: InferrableType,
      A4: InferrableType,
      A5: InferrableType,
      A6: InferrableType](f: (A1, A2, A3, A4, A5, A6) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6) => R]): InferrableFunction = InferrableFunction(
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

  def nullTolerantInferrableFunction7[
      R: InferrableType,
      A1: InferrableType,
      A2: InferrableType,
      A3: InferrableType,
      A4: InferrableType,
      A5: InferrableType,
      A6: InferrableType,
      A7: InferrableType](f: (A1, A2, A3, A4, A5, A6, A7) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7) => R]): InferrableFunction =
    InferrableFunction(
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

  def nullTolerantInferrableFunction8[
      R: InferrableType,
      A1: InferrableType,
      A2: InferrableType,
      A3: InferrableType,
      A4: InferrableType,
      A5: InferrableType,
      A6: InferrableType,
      A7: InferrableType,
      A8: InferrableType](f: (A1, A2, A3, A4, A5, A6, A7, A8) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8) => R]): InferrableFunction =
    InferrableFunction(
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

  def nullTolerantInferrableFunction9[
      R: InferrableType,
      A1: InferrableType,
      A2: InferrableType,
      A3: InferrableType,
      A4: InferrableType,
      A5: InferrableType,
      A6: InferrableType,
      A7: InferrableType,
      A8: InferrableType,
      A9: InferrableType](f: (A1, A2, A3, A4, A5, A6, A7, A8, A9) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9) => R]): InferrableFunction =
    InferrableFunction(
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

  def nullTolerantInferrableFunction10[
      R: InferrableType,
      A1: InferrableType,
      A2: InferrableType,
      A3: InferrableType,
      A4: InferrableType,
      A5: InferrableType,
      A6: InferrableType,
      A7: InferrableType,
      A8: InferrableType,
      A9: InferrableType,
      A10: InferrableType](f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) => R]): InferrableFunction =
    InferrableFunction(
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

  def nullTolerantInferrableFunction11[
      R: InferrableType,
      A1: InferrableType,
      A2: InferrableType,
      A3: InferrableType,
      A4: InferrableType,
      A5: InferrableType,
      A6: InferrableType,
      A7: InferrableType,
      A8: InferrableType,
      A9: InferrableType,
      A10: InferrableType,
      A11: InferrableType](f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11) => R]): InferrableFunction =
    InferrableFunction(
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

  def nullTolerantInferrableFunction12[
      R: InferrableType,
      A1: InferrableType,
      A2: InferrableType,
      A3: InferrableType,
      A4: InferrableType,
      A5: InferrableType,
      A6: InferrableType,
      A7: InferrableType,
      A8: InferrableType,
      A9: InferrableType,
      A10: InferrableType,
      A11: InferrableType,
      A12: InferrableType](f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12) => R])
      : InferrableFunction = InferrableFunction(
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

  def nullTolerantInferrableFunction13[
      R: InferrableType,
      A1: InferrableType,
      A2: InferrableType,
      A3: InferrableType,
      A4: InferrableType,
      A5: InferrableType,
      A6: InferrableType,
      A7: InferrableType,
      A8: InferrableType,
      A9: InferrableType,
      A10: InferrableType,
      A11: InferrableType,
      A12: InferrableType,
      A13: InferrableType](f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13) => R)(
      implicit typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13) => R])
      : InferrableFunction = InferrableFunction(
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

  def nullTolerantInferrableFunction14[
      R: InferrableType,
      A1: InferrableType,
      A2: InferrableType,
      A3: InferrableType,
      A4: InferrableType,
      A5: InferrableType,
      A6: InferrableType,
      A7: InferrableType,
      A8: InferrableType,
      A9: InferrableType,
      A10: InferrableType,
      A11: InferrableType,
      A12: InferrableType,
      A13: InferrableType,
      A14: InferrableType](
      f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14) => R])
      : InferrableFunction = InferrableFunction(
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

  def nullTolerantInferrableFunction15[
      R: InferrableType,
      A1: InferrableType,
      A2: InferrableType,
      A3: InferrableType,
      A4: InferrableType,
      A5: InferrableType,
      A6: InferrableType,
      A7: InferrableType,
      A8: InferrableType,
      A9: InferrableType,
      A10: InferrableType,
      A11: InferrableType,
      A12: InferrableType,
      A13: InferrableType,
      A14: InferrableType,
      A15: InferrableType](
      f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15) => R)(implicit
      typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15) => R])
      : InferrableFunction = InferrableFunction(
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

  def nullTolerantInferrableFunction16[
      R: InferrableType,
      A1: InferrableType,
      A2: InferrableType,
      A3: InferrableType,
      A4: InferrableType,
      A5: InferrableType,
      A6: InferrableType,
      A7: InferrableType,
      A8: InferrableType,
      A9: InferrableType,
      A10: InferrableType,
      A11: InferrableType,
      A12: InferrableType,
      A13: InferrableType,
      A14: InferrableType,
      A15: InferrableType,
      A16: InferrableType](
      f: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16) => R)(implicit
  typeTag: TypeTag[(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16) => R])
      : InferrableFunction = InferrableFunction(
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
