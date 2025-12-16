package lab1

import kotlin.random.Random

data class ResultCell(val i: Int, val k: Int)
data class Val(val tag: Char, val j: Int, val value: Double) // ('A', j, value)


fun mapperA(A: Array<DoubleArray>, colsB: Int): List<Pair<ResultCell, Val>> {
    val rows = A.size
    val cols = A[0].size
    val result = ArrayList<Pair<ResultCell, Val>>(rows * cols * colsB)
    for (i in 0 until rows) {
        for (j in 0 until cols) {
            val cellValue = A[i][j]
            for (k in 0 until colsB) {
                result += ResultCell(i, k) to Val('A', j, cellValue) // ((i, k), ('A', j, value))
            }
        }
    }
    return result
}

fun mapperB(B: Array<DoubleArray>, rowsA: Int): List<Pair<ResultCell, Val>> {
    val rows = B.size
    val cols = B[0].size
    val result = ArrayList<Pair<ResultCell, Val>>(rows * cols * rowsA)
    for (j in 0 until rows) {
        for (k in 0 until cols) {
            val cellValue = B[j][k]
            for (i in 0 until rowsA) {
                result += ResultCell(i, k) to Val('B', j, cellValue)
            }
        }
    }
    return result
}


fun shuffler(mapped: List<Pair<ResultCell, Val>>): Map<ResultCell, List<Val>> {
    val sorted = mapped.sortedWith(
        compareBy<Pair<ResultCell, Val>> { it.first.i }
            .thenBy { it.first.k }
            .thenBy { it.second.tag }
            .thenBy { it.second.j }
    )
    val result = LinkedHashMap<ResultCell, MutableList<Val>>()
    for ((key, value) in sorted) {
        result.getOrPut(key) { ArrayList() }.add(value)
    }
    return result
}

fun reducer(
    grouped: Map<ResultCell, List<Val>>,
    rowsC: Int,
    colsC: Int
): Array<DoubleArray> {
    val C = Array(rowsC) { DoubleArray(colsC) }
    for ((resultCell, list) in grouped) {
        val rowValuesA = HashMap<Int, Double>()
        val columnValuesB = HashMap<Int, Double>()
        for (value in list) {
            when (value.tag) {
                'A' -> rowValuesA[value.j] = value.value
                'B' -> columnValuesB[value.j] = value.value
            }
        }
        var sum = 0.0
        for ((j, valueA) in rowValuesA) {
            columnValuesB[j]?.let { valueB ->
                sum += valueA * valueB
            }
        }
        C[resultCell.i][resultCell.k] = sum
    }
    return C
}

fun multiplyMapReduce(A: Array<DoubleArray>, B: Array<DoubleArray>): Array<DoubleArray> {
    val rowsA = A.size
    val colsA = A[0].size
    val rowsB = B.size
    val colsB = B[0].size
    require( colsA == rowsB) { "Несогласованные размерности: A - ${rowsA}x${colsA}, B - ${rowsB}x${colsB}" }

    val mappedA = mapperA(A, colsB)
    val mappedB = mapperB(B, rowsA)
    val grouped = shuffler(mappedA + mappedB)
    return reducer(grouped, rowsA, colsB)
}

fun randomMatrix(rows: Int, cols: Int, seed: Int = 42): Array<DoubleArray> {
    val rnd = Random(seed)
    return Array(rows) { DoubleArray(cols) { rnd.nextDouble(-1.0, 1.0) } }
}


fun main() {

    val rowsA = 200
    val colsA = 300
    val colsB = 150

    println("A = ${rowsA}x${colsA}, B = ${colsA}x${colsB}")

    val A = randomMatrix(rowsA, colsA, seed = 1)

    println("\nA\n")
    for (row in A) println(row.joinToString(prefix = "[", postfix = "]") { "%.1f".format(it) })

    val B = randomMatrix(colsA, colsB, seed = 2)

    println("\nB\n")
    for (row in B) println(row.joinToString(prefix = "[", postfix = "]") { "%.1f".format(it) })

    val C = multiplyMapReduce(A, B)

    println("\nC = A × B\n")
    for (row in C) println(row.joinToString(prefix = "[", postfix = "]") { "%.1f".format(it) })
}