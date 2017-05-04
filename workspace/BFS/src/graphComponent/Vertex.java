package graphComponent;



import java.io.Serializable;
import java.util.*;


/**
 * Vertex is used for MapReduce Graph processing and storing intermediate results in format:
 * <p/>
 * | ID |  Neighbours  |    Path   | Distance | Color |
 * -----------------------------------------------------
 * |  3 |    [2, 4, 5] | [0, 5, 3] |     2    | BLACK |
 * |  5 |       [0, 3] |    [0, 5] |     1    | BLACK |
 * |  1 |       [0, 2] |    [0, 1] |     1    | BLACK |
 * |  0 |    [1, 2, 5] |       [0] |     0    | BLACK |
 * |  2 | [0, 1, 3, 4] |    [0, 2] |     1    | BLACK |
 * |  4 |       [2, 3] | [0, 2, 4] |     2    | BLACK |
 *
 * @see Color
 */
public final class Vertex {

    private static final String BAR_SEPARATOR = "|";
    private static final String TAB_SEPARATOR = "\t";
    private static final String INTMAX = "Integer.MAX";

    private final int id;

    private Set<Integer> neighbours;

//    private List<Integer> path;

    private int distance;

    private Color color;

    public Vertex(int id, Set<Integer> neighbours,  int distance, Color color) {
        this.id = id;
        this.neighbours = neighbours;
//        this.path = path;
        this.distance = distance;
        this.color = color;
    }

    public int getId() {
        return id;
    }

    public Set<Integer> getNeighbours() {
        return Collections.unmodifiableSet(neighbours);
    }

    public void addNeighbour(int vertex) {
        neighbours.add(vertex);
    }

//    public List<Integer> getPath() {
//        return Collections.unmodifiableList(path);
//    }

    public int getDistance() {
        return distance;
    }

    public Color getColor() {
        return color;
    }

    public void setColor(Color color) {
        this.color = color;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        if (o instanceof Vertex) {
            Vertex object = (Vertex) o;

            return Objects.equals(id, object.id) &&
                    Objects.equals(neighbours, object.neighbours) &&
//                    Objects.equals(path, object.path) &&
                    Objects.equals(distance, object.distance) &&
                    Objects.equals(color, object.color);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, neighbours, distance, color);
    }

    /**
     * Provides the source to regenerate a vertex from
     *
     * @return vertex string
     */
    @Override
    public String toString() {
    	String dist;
    	if (distance == Integer.MAX_VALUE) {
    		dist = INTMAX;
    	} else {
    		dist = String.valueOf(distance);
    	}
    	String adjacent = "";
    	for (int x : neighbours) {
    		if (adjacent == "") {
    			adjacent = adjacent + String.valueOf(x);
    			continue;
    		}
    		adjacent = adjacent + "," + String.valueOf(x); 		
    	}
    	
        return id + TAB_SEPARATOR + adjacent + BAR_SEPARATOR + dist + BAR_SEPARATOR + color;
    }
}