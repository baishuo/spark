package org.apache.spark.sql.hive
import scala.collection.mutable.{Stack, ArrayBuffer}

class Bitmap {
    private var bmp: BigInt = 0

    private def this(bmp: BigInt) { this; this.bmp = bmp }

    def setOne(i: Int): Bitmap = { bmp = bmp.setBit(i); this }

    def setZero(i: Int): Bitmap = { bmp &~= BigInt(1) << i; this }

    def testOne(i: Int): Boolean = bmp.testBit(i)

    def testZero(i: Int): Boolean = !testOne(i)

    def testSubset(bitmap: Bitmap): Boolean = 0 == (bmp &~ bitmap.bmp)

    def testOverlap(bitmap: Bitmap): Boolean = (bmp & bitmap.bmp) != 0

    def ==(bitmap: Bitmap): Boolean = bmp.equals(bitmap.bmp)

    def !=(bitmap: Bitmap): Boolean = !(this == bitmap)

    def &=(bitmap: Bitmap): Bitmap = { bmp &= bitmap.bmp; this }

    def |=(bitmap: Bitmap): Bitmap = { bmp |= bitmap.bmp; this }

    def &(bitmap: Bitmap): Bitmap = new Bitmap(bmp) &= bitmap

    def |(bitmap: Bitmap): Bitmap = new Bitmap(bmp) |= bitmap

    def width: Int = bmp.toString(2).length

    def bits: Int = bmp.bitCount

    override def toString: String = ("%s: %s").format(bmp.toString, bmp.toString(2))
}

object ExprType extends Enumeration {
    type ExprType = Value

    val ET_COLUMN = Value("")
    val ET_CUBE = Value("cube(")
    val ET_ROLLUP = Value("rollup(")
    val ET_GROUPING = Value("groupingsets(")
    val ET_COMBINATION = Value("(")
    val ET_INVALID = Value("")
}

import ExprType._

class Expr(val etype: ExprType = ET_INVALID) {
    val args: ArrayBuffer[AnyRef] = new ArrayBuffer[AnyRef]

    def +=(arg: AnyRef) = args += arg

    def |=(expr: Expr) = for (arg <- expr.args) args += arg

    override def toString: String = args.toString
}

class GrpParser private[this] {
    private[this] var src: String = ""
    private[this] var grp: String = ""
    private[this] val cols: ArrayBuffer[String] = new ArrayBuffer[String]

    def this(grp: String) {
        this
        this.src = grp
        this.grp = grp.replaceAll(" ", "").toLowerCase
    }

    private[this] def getType:  ExprType = {
        var nType: ExprType = ET_INVALID

        if (grp.startsWith(ET_CUBE.toString)) {
            nType = ET_CUBE
        } else if (grp.startsWith(ET_ROLLUP.toString)) {
            nType = ET_ROLLUP
        } else if (grp.startsWith(ET_GROUPING.toString)) {
            nType = ET_GROUPING
        } else if (grp.startsWith(ET_COMBINATION.toString)) {
            nType = ET_COMBINATION
        } else if (grp.length > 0 && grp(0).isLetter){
            nType = ET_COLUMN
        }

        nType
    }

    private[this] def doGetNext: String = {
        val stack: Stack[Char] = Stack[Char]()
        var idx: Int = 0
        var next: String = ""

        stack.push('(')
        for (c <- grp if !stack.isEmpty) {
            if ('(' == c) stack.push(c) else if (')' == c) stack.pop
            if (stack.length > 2) throw new RuntimeException("Deeply nested combination is not enabled.")
            idx += 1
        }

        if (!stack.isEmpty) throw new RuntimeException("Syntax error, miss ')'.")
        if (grp.length != idx && ',' != grp(idx)) throw new RuntimeException("Syntax error, check input.")

        next = grp.take(idx - 1)
        grp = if (grp.length == idx) "" else grp.takeRight(grp.length - idx - 1)

        next
    }

    private def getNext: (String, ExprType) = {
        var next: String = ""
        val nType: ExprType = getType
        var ret: (String, ExprType) = ("", ET_INVALID)

        nType match {
            case ET_INVALID => if (grp.length > 0) throw new RuntimeException("Syntax error, invalid identifier.")
            case ET_COLUMN =>
                val aSplits: Array[String] = grp.split(",", 2)

                next = aSplits(0)
                grp = if (aSplits.length > 1) aSplits(1) else ""
            case _ =>
                grp = grp.takeRight(grp.length - nType.toString.length)
                next = doGetNext

                if (0 == next.length) throw new RuntimeException(("Syntax error, args of %s) can not be empty.").format(nType.toString))
        }

        if (ET_GROUPING == nType) {
            if (next.contains(nType.toString)) throw new RuntimeException("Nested grouping sets is not enabled.")
        } else {
            val ex: Array[ExprType] = Array[ExprType](ET_CUBE, ET_ROLLUP, ET_GROUPING)

            for (t <- ex) {
                val msg: String = ("%s) can not be contained in %s.").format(t.toString, next)
                if (next.contains(t.toString)) throw new RuntimeException(msg)
            }
        }

        ret = (next, nType)
        ret
    }

    private[this] def col2expr(input: String): Expr = {
        val e = new Expr(ET_COLUMN)
        var i = cols.indexOf(input)
        val bmp = new Bitmap

        if (i < 0) {
            cols += input
            i = cols.length - 1
        }

        e += bmp.setOne(i)
        e
    }

    private[this] def othr2expr(input: String, etype: ExprType): Expr = {
        var e = new Expr(etype)
        val p = new GrpParser(input)
        var n = p.getNext

        while (n._2 != ET_INVALID) {
            n._2 match {
                case ET_COLUMN => e += col2expr(n._1)
                case _ => e += othr2expr(n._1, n._2)
            }

            n = p.getNext
        }

        if (ET_COMBINATION == etype) {
            val bmp = new Bitmap

            for (arg <- e.args) {
                var se: Expr = null

                assert(arg.isInstanceOf[Expr])
                se = arg.asInstanceOf[Expr]

                assert((ET_COLUMN == se.etype || ET_COMBINATION == se.etype) && 1 == se.args.length)
                bmp |= se.args(0).asInstanceOf[Bitmap]
            }

            e = new Expr(etype)
            e += bmp
        }

        e
    }

    private[this] def grp2expr: ArrayBuffer[Expr] = {
        val es: ArrayBuffer[Expr] = new ArrayBuffer[Expr]
        var n = getNext

        while (n._2 != ET_INVALID) {
            n._2 match {
                case ET_COLUMN => es += col2expr(n._1)
                case _ => es += othr2expr(n._1, n._2)
            }

            n = getNext
        }

        //println(cols.toString) //just for debug
        es
    }

    private[this] def extract(value: BigInt, args: ArrayBuffer[AnyRef]): Bitmap = {
        val bmp: Bitmap = new Bitmap

        for (i <- 0 until value.toString(2).length if value.testBit(i)) {
            assert(args(i).isInstanceOf[Expr])
            val e: Expr = args(i).asInstanceOf[Expr]

            assert(1 == e.args.length && e.args(0).isInstanceOf[Bitmap])
            val b: Bitmap = e.args(0).asInstanceOf[Bitmap]

            bmp |= b
        }

        bmp
    }

    private[this] def cube2combine(e: Expr): Expr = {
        val ret: Expr = new Expr(ET_COMBINATION)

        assert(e.args.length > 0)
        for (i <- BigInt(0).until(BigInt(1) << e.args.length)) ret += extract(i, e.args)

        ret
    }

    private[this] def rollup2combine(e: Expr): Expr = {
        val ret: Expr = new Expr(ET_COMBINATION)
        var value: BigInt = BigInt(0)

        assert(e.args.length > 0)
        for (i <- 0 until e.args.length) value = value.setBit(i)

        while (value > 0) {
            ret += extract(value, e.args)
            value >>= 1
        }

        ret += new Bitmap
        ret
    }

    private[this] def grpsets2combine(e: Expr): Expr = {
        val ret: Expr = new Expr(ET_COMBINATION)

        for (arg <- e.args) {
            assert(arg.isInstanceOf[Expr])
            val se: Expr = arg.asInstanceOf[Expr]

            se.etype match {
                case ET_COLUMN | ET_COMBINATION => ret |= se
                case ET_CUBE => ret |= cube2combine(se)
                case ET_ROLLUP => ret |= rollup2combine(se)
                case _ => throw new RuntimeException("Syntax error, grouping sets contains invalid phrases.")
            }
        }

        ret
    }

    private[this] def eproduct(l: Expr, r: Expr): Expr = {
        val ret: Expr = new Expr(ET_COMBINATION)

        assert(r.args.length > 0)
        if (0 == l.args.length) ret |= r

        for (larg <- l.args) {
            assert(larg.isInstanceOf[Bitmap])
            val lbmp: Bitmap = larg.asInstanceOf[Bitmap]

            for (rarg <- r.args) {
                assert(rarg.isInstanceOf[Bitmap])
                val rbmp: Bitmap = rarg.asInstanceOf[Bitmap]

                ret += (lbmp | rbmp)
            }
        }

        ret
    }

    private[this] def grp2combine(grps: ArrayBuffer[Expr]): Expr = {
        var ret: Expr = new Expr(ET_COMBINATION)

        for (e <- grps) {
            assert(e.isInstanceOf[Expr])

            e.etype match {
                case ET_COLUMN | ET_COMBINATION => ret = eproduct(ret, e)
                case ET_CUBE => ret = eproduct(ret, cube2combine(e))
                case ET_ROLLUP => ret = eproduct(ret, rollup2combine(e))
                case ET_GROUPING => ret = eproduct(ret, grpsets2combine(e))
                case _ => throw new RuntimeException("Syntax error, grouping sets contains invalid phrases.")
            }
        }

        ret
    }

    private[this] def revert(bitmap: Bitmap): String = {
        var ret: String = ""

        for (i <- 0 until bitmap.width if bitmap.testOne(i)) ret += ("%s, ").format(cols(i))
        (if (1 == bitmap.bits) "%s, " else "(%s), ").format(if (ret.length > 1) ret.dropRight(2) else ret)
    }

    private[this] def doParser: String = {
        var ret: String = ""
        val e: Expr = grp2combine(grp2expr)

        for (arg <- e.args) {
            assert(arg.isInstanceOf[Bitmap])
            ret += revert(arg.asInstanceOf[Bitmap])
        }

        ("%s grouping sets(%s)").format(cols.mkString(", "), ret.dropRight(2))
    }

    def parser: String = {
        if (grp.contains(ET_CUBE.toString)
            || grp.contains(ET_ROLLUP.toString)
            || grp.contains(ET_GROUPING.toString)) doParser else src
    }
}
