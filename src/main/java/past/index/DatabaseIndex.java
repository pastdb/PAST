package past.index;

import java.util.List;

/**
 * Interface of a database index. The index 
 *
 * @param <T> is the type of values indexed.
 */
public interface DatabaseIndex<T> {
	
	/**
	 * Build the index.
	 * 
	 * @return if the index has been built correctly.
	 */
	public boolean buildIndex();
	
	/**
	 * Checks if the index is built correctly.
	 * 
	 * @return true if the index is built.
	 */
	public boolean isBuilt();

	/**
	 * Computes the nearest neighbors of a given point.
	 * 
	 * @param numberOfNeighbors the number of neighbors to compute.
	 * @param point coordinates of point to which compute neighbors.
	 * 
	 * @return the nearest neighbors.
	 */
	public List<T> nearestNeighbors(int numberOfNeighbors, T point);
}
