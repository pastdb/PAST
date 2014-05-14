package past.index;

import java.io.Serializable;
import java.util.Arrays;

import scala.Tuple2;

/**
 * Simple container class for a vector and its name/pointer.
 */
public class NamedVector implements Serializable {
	
	private static final long serialVersionUID = -3491658067866225900L;
	private int[] ords;
	private String name;
	
	public NamedVector(String name, int[] ords) {
		if (name == null || name.length() < 1 || ords == null || ords.length < 1) {
			throw new IllegalArgumentException();
		}
		
		this.setName(name);
		this.setOrds(Arrays.copyOf(ords, ords.length));
	}
	
	public NamedVector(Tuple2<String, int[]> vector) {
		if (vector == null) {
			throw new IllegalArgumentException();
		}
		
		this.setName(vector._1());
		this.setOrds(Arrays.copyOf(vector._2(), vector._2().length));
	}

    @Override
    public boolean equals(Object o) {
        NamedVector nv = (NamedVector) o;
        return nv.getName().equals(this.name) && Arrays.equals(nv.getOrds(), this.ords);
    }

	public int[] getOrds() {
		return ords;
	}

	public void setOrds(int[] ords) {
		this.ords = ords;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
