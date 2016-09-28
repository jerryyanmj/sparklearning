import waterpouring.Pouring

val t = 9

val problem = new Pouring(Vector(4, 9, 19))

problem.moves

problem.pathSets.take(3).toList

problem.solutions(3)

problem.cubeSolution