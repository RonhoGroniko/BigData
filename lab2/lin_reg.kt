package lab1

import kotlin.math.abs

fun transpose(M: Array<DoubleArray>): Array<DoubleArray> {
    val rows = M.size
    val cols = M[0].size
    val result = Array(cols) { DoubleArray(rows) }
    for (i in 0 until rows) {
        for (j in 0 until cols) {
            result[j][i] = M[i][j]
        }
    }
    return result
}

fun addIntercept(X: Array<DoubleArray>): Array<DoubleArray> {
    val rows = X.size
    val cols = X[0].size
    val result = Array(rows) { DoubleArray(cols + 1) }
    for (i in 0 until rows) {
        result[i][0] = 1.0
        for (j in 0 until cols) {
            result[i][j + 1] = X[i][j]
        }
    }
    return result
}

fun toColumnMatrix(v: DoubleArray): Array<DoubleArray> =
    Array(v.size) { i -> doubleArrayOf(v[i]) }

fun fromColumnMatrix(M: Array<DoubleArray>): DoubleArray {
    require(M[0].size == 1) { "Ожидается матрица-столбец (p×1)" }
    return DoubleArray(M.size) { i -> M[i][0] }
}


fun solveLinearSystem(A: Array<DoubleArray>, b: DoubleArray): DoubleArray {
    val n = A.size
    require(A.all { it.size == n }) { "A должна быть квадратной n×n" }
    require(b.size == n) { "Размер b должен быть n" }

    // Формируем расширенную матрицу [A | b]
    val a = Array(n) { i ->
        DoubleArray(n + 1) { j ->
            if (j < n) A[i][j] else b[i]
        }
    }

    // Прямой ход с частичным выбором
    for (col in 0 until n) {
        // Выбор строки с максимальным |a[row][col]|
        var pivot = col
        var best = abs(a[pivot][col])
        for (row in col + 1 until n) {
            val v = abs(a[row][col])
            if (v > best) {
                best = v
                pivot = row
            }
        }
        require(best != 0.0) { "Матрица вырождена или близка к сингулярной" }

        // Поменять местами текущую строку и pivot
        if (pivot != col) {
            val tmp = a[col]
            a[col] = a[pivot]
            a[pivot] = tmp
        }

        // Нормализуем ведущую строку
        val lead = a[col][col]
        for (j in col until n + 1) {
            a[col][j] /= lead
        }

        // Обнулить элементы ниже
        for (row in col + 1 until n) {
            val factor = a[row][col]
            if (factor == 0.0) continue
            for (j in col until n + 1) {
                a[row][j] -= factor * a[col][j]
            }
        }
    }

    // Обратный ход
    val x = DoubleArray(n)
    for (row in n - 1 downTo 0) {
        var sum = a[row][n]
        for (j in row + 1 until n) {
            sum -= a[row][j] * x[j]
        }
        x[row] = sum
    }
    return x
}


data class LinearRegressionResult(
    val weights: DoubleArray,
    val intercept: Boolean
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as LinearRegressionResult

        if (intercept != other.intercept) return false
        if (!weights.contentEquals(other.weights)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = intercept.hashCode()
        result = 31 * result + weights.contentHashCode()
        return result
    }
}

// (Xᵀ · X) · w = Xᵀ · y
fun linearRegressionNormalEq(
    Xraw: Array<DoubleArray>,
    y: DoubleArray,
    intercept: Boolean = true
): LinearRegressionResult {
    require(Xraw.size == y.size) { "Число строк X (${Xraw.size}) должно равняться длине y (${y.size})" }

    val X = if (intercept) addIntercept(Xraw) else Xraw

    val Xt = transpose(X)
    val XtX = multiplyMapReduce(Xt, X)
    val Ycol = toColumnMatrix(y)
    val XtYcol = multiplyMapReduce(Xt, Ycol)
    val XtY = fromColumnMatrix(XtYcol)

    val w = solveLinearSystem(XtX, XtY)
    return LinearRegressionResult(weights = w, intercept = intercept)
}


fun demoLinearRegression() {
    val Xraw = arrayOf(
        doubleArrayOf(1.0, 2.0),
        doubleArrayOf(2.0, 0.0),
        doubleArrayOf(3.0, 1.0),
        doubleArrayOf(4.0, 3.0),
        doubleArrayOf(5.0, 2.0)
    )

    val y = doubleArrayOf(6.1, 7.0, 9.4, 12.6, 13.1)

    val model = linearRegressionNormalEq(Xraw, y, intercept = true)
    val w = model.weights

    println("Линейная регрессия:")
    println("w0 (intercept) = ${"%.4f".format(w[0])}")
    println("w1 = ${"%.4f".format(w[1])}, w2 = ${"%.4f".format(w[2])}")

    val xTest = doubleArrayOf(6.0, 1.0)
    val yHat = w[0] + w[1] * xTest[0] + w[2] * xTest[1]
    println("Прогноз для набора признаков: x1 = 6.0, x2 = 1.0: ŷ = ${"%.4f".format(yHat)}")
}


fun main() {
    demoLinearRegression()
}