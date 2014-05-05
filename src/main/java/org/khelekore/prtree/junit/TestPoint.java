//package org.khelekore.prtree.junit;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import org.khelekore.prtree.MBR;
//import org.khelekore.prtree.MBRConverter;
//import org.khelekore.prtree.PRTree;
//import org.khelekore.prtree.PointND;
//import org.khelekore.prtree.SimpleMBR;
//import org.khelekore.prtree.SimpleMBR2D;
//import org.khelekore.prtree.SimplePointND;
//
//public class TestPoint {
//
//	public static void main(String[] args) {
//		class PointNDConverter implements MBRConverter<PointND> {
//			private int dimension;
//			
//			PointNDConverter(int dimension) {
//				this.dimension = dimension;
//			}
//			
//			@Override
//			public int getDimensions() {
//				return this.dimension;
//			}
//
//			@Override
//			public double getMin(int axis, PointND t) {
//				return t.getOrd(axis);
//			}
//
//			@Override
//			public double getMax(int axis, PointND t) {
//				return t.getOrd(axis);
//			}
//			
//		}
//		
//		List<PointND> points = new ArrayList<PointND>();
//		points.add(new SimplePointND(4,5, 4, 6));
//		points.add(new SimplePointND(1,1,1,1));
//		points.add(new SimplePointND(4,6,4,5));
//		points.add(new SimplePointND(3,2,3,2));
//
//		
//		PRTree<PointND> tree = new PRTree<PointND>(new PointNDConverter(4), 30);
//		tree.load(points);
//		MBR mbr = new SimpleMBR(0, 7, 0, 7, 0, 7, 0,7);
//		
//		
//		for (PointND point : tree.find(mbr)) {
//			System.out.println(point.getOrd(0)+", "+point.getOrd(1));
//		}
//	}
//
//}
