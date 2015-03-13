// One iteration of authority rank algorithm
def authorityRank(g: Graph[V, E]) = {
	// Calculate D: Propagate R[i,j] values and sum them at each vertex
	val D = g.aggregateMessages(edgeContext => {
			edgeContext.sendToDst({
				val array: Array[Double](numTypes, numClasses)
				array[srcType] = edgeContext.attr.R
				return array
			})
			edgeContext.sendToSrc({
				val array: Array[Double](numTypes, numClasses)
				array[dstType] = edgeContext.attr.R
				return array
			})
		},
		(array1, array2) => sum(array1, array2)
	)

	// Calculate S
	g = g.joinVertices(D).mapTriplets(triplet => {
		for (k <- 0 until numClasses) {
			val srcNormalize = 1 / sqrt(src.D(dst.type))
			val dstNormalize = 1 / sqrt(dst.D(src.type))
			newEdge.S(k) = srcNormalize * edge.R(k) * dstNormalize
		}
		return newEdge
	})

	// Calculate P(x_ip | T_i, k)^t : Part 1
	// Calculate Sum( lambda * S * P^{t-1} )
	val rankUpdates = g.aggregateMessages(edgeContext => {
			val dstMsg, srcMsg = Array.ofDim[Double](numClasses)
			for (k <- 0 until numClasses) {
				dstMsg(k) = lambda(src.type)(dst.type) * S(k) * P_src(k)
				srcMsg(k) = lambda(dst.type)(src.type) * S(k) * P_dst(k)
			}
			edgeContext.sendToDst(dstMsg)
			edgeContext.sendToSrc(srcMsg)
		},
		(array1, array2) => sum(array1, array2)
	)

	// Calculate P(x_ip | T_i, k)^t : Part 2
	// Add initial label info. and normalize the result
	g = g.joinVertices(rankUpdates)((_, v, P_prev) => {
		for (k <- 0 until numClasses) {
			v.P(k) = (P_prev(k) + alpha(v.type) * v.P_0(k)) / sum(lambda, alpha)
		}
		return v
	})

	return g
}