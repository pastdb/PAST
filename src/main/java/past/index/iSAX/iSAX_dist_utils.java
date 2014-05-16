package past.index.iSAX;

public class iSAX_dist_utils{

//** iSAX breakpoints depending on cardinalities as in the iSAXpaper

  public static final double[] breakpoint_card_2 = { 0 };
  public static final double[] breakpoint_card_3 = { -0.43, 0.43 };
  public static final double[] breakpoint_card_4 = { -0.67, 0, 0.67 };
  public static final double[] breakpoint_card_5 = { -0.84, -0.25, 0.25, 0.84 };
  public static final double[] breakpoint_card_6 = { -0.97, -0.43, 0.00, 0.43, 0.97 };
  public static final double[] breakpoint_card_7 = { -1.07, -0.57, -0.18, 0.18, 0.57, 1.07 };
  public static final double[] breakpoint_card_8 = { -1.15, -0.67, -0.32, 0.00, 0.32, 0.67, 1.15 };
  public static final double[] breakpoint_card_9 = { -1.22, -0.76, -0.43, -0.14, 0.14, 0.43, 0.76, 1.22 };
  public static final double[] breakpoint_card_10 = { -1.28, -0.84, -0.52, -0.25, 0.00, 0.25, 0.52,0.84, 1.28 };
  
  /**
The lookuptable and distance tables were generated using the matlab code provided on the SAX website 
TODO add more possibilities for cardinalitites
*/
//**SAX distance lookuptable
  private static final double[][] lookup_case2 = {
     { 0.000000, 0.000000 },
     { 0.000000, 0.000000 } };


  private static final double[][] lookup_case4 = {
   { 0.000000, 0.000000, 0.670000, 1.340000 }, 
   { 0.000000, 0.000000, 0.000000, 0.670000 },
      { 0.670000, 0.000000, 0.000000, 0.000000 },
       { 1.340000, 0.670000, 0.000000, 0.000000 } };



  private static final double[][] lookup_case8 = {
      { 0.000000, 0.000000, 0.480000, 0.830000, 1.150000, 1.470000, 1.820000, 2.300000 },
      { 0.000000, 0.000000, 0.000000, 0.350000, 0.670000, 0.990000, 1.340000, 1.820000 },
      { 0.480000, 0.000000, 0.000000, 0.000000, 0.320000, 0.640000, 0.990000, 1.470000 },
      { 0.830000, 0.350000, 0.000000, 0.000000, 0.000000, 0.320000, 0.670000, 1.150000 },
      { 1.150000, 0.670000, 0.320000, 0.000000, 0.000000, 0.000000, 0.350000, 0.830000 },
      { 1.470000, 0.990000, 0.640000, 0.320000, 0.000000, 0.000000, 0.000000, 0.480000 },
      { 1.820000, 1.340000, 0.990000, 0.670000, 0.350000, 0.000000, 0.000000, 0.000000 },
      { 2.300000, 1.820000, 1.470000, 1.150000, 0.830000, 0.480000, 0.000000, 0.000000 } };


	  private static final double[][] lookup_case16 = {
      { 0.000000, 0.000000, 0.380000, 0.640000, 0.860000, 1.040000, 1.210000, 1.370000, 1.530000,
          1.690000, 1.850000, 2.020000, 2.200000, 2.420000, 2.680000, 3.060000 },
      { 0.000000, 0.000000, 0.000000, 0.260000, 0.480000, 0.660000, 0.830000, 0.990000, 1.150000,
          1.310000, 1.470000, 1.640000, 1.820000, 2.040000, 2.300000, 2.680000 },
      { 0.380000, 0.000000, 0.000000, 0.000000, 0.220000, 0.400000, 0.570000, 0.730000, 0.890000,
          1.050000, 1.210000, 1.380000, 1.560000, 1.780000, 2.040000, 2.420000 },
      { 0.640000, 0.260000, 0.000000, 0.000000, 0.000000, 0.180000, 0.350000, 0.510000, 0.670000,
          0.830000, 0.990000, 1.160000, 1.340000, 1.560000, 1.820000, 2.200000 },
      { 0.860000, 0.480000, 0.220000, 0.000000, 0.000000, 0.000000, 0.170000, 0.330000, 0.490000,
          0.650000, 0.810000, 0.980000, 1.160000, 1.380000, 1.640000, 2.020000 },
      { 1.040000, 0.660000, 0.400000, 0.180000, 0.000000, 0.000000, 0.000000, 0.160000, 0.320000,
          0.480000, 0.640000, 0.810000, 0.990000, 1.210000, 1.470000, 1.850000 },
      { 1.210000, 0.830000, 0.570000, 0.350000, 0.170000, 0.000000, 0.000000, 0.000000, 0.160000,
          0.320000, 0.480000, 0.650000, 0.830000, 1.050000, 1.310000, 1.690000 },
      { 1.370000, 0.990000, 0.730000, 0.510000, 0.330000, 0.160000, 0.000000, 0.000000, 0.000000,
          0.160000, 0.320000, 0.490000, 0.670000, 0.890000, 1.150000, 1.530000 },
      { 1.530000, 1.150000, 0.890000, 0.670000, 0.490000, 0.320000, 0.160000, 0.000000, 0.000000,
          0.000000, 0.160000, 0.330000, 0.510000, 0.730000, 0.990000, 1.370000 },
      { 1.690000, 1.310000, 1.050000, 0.830000, 0.650000, 0.480000, 0.320000, 0.160000, 0.000000,
          0.000000, 0.000000, 0.170000, 0.350000, 0.570000, 0.830000, 1.210000 },
      { 1.850000, 1.470000, 1.210000, 0.990000, 0.810000, 0.640000, 0.480000, 0.320000, 0.160000,
          0.000000, 0.000000, 0.000000, 0.180000, 0.400000, 0.660000, 1.040000 },
      { 2.020000, 1.640000, 1.380000, 1.160000, 0.980000, 0.810000, 0.650000, 0.490000, 0.330000,
          0.170000, 0.000000, 0.000000, 0.000000, 0.220000, 0.480000, 0.860000 },
      { 2.200000, 1.820000, 1.560000, 1.340000, 1.160000, 0.990000, 0.830000, 0.670000, 0.510000,
          0.350000, 0.180000, 0.000000, 0.000000, 0.000000, 0.260000, 0.640000 },
      { 2.420000, 2.040000, 1.780000, 1.560000, 1.380000, 1.210000, 1.050000, 0.890000, 0.730000,
          0.570000, 0.400000, 0.220000, 0.000000, 0.000000, 0.000000, 0.380000 },
      { 2.680000, 2.300000, 2.040000, 1.820000, 1.640000, 1.470000, 1.310000, 1.150000, 0.990000,
          0.830000, 0.660000, 0.480000, 0.260000, 0.000000, 0.000000, 0.000000 },
      { 3.060000, 2.680000, 2.420000, 2.200000, 2.020000, 1.850000, 1.690000, 1.530000, 1.370000,
          1.210000, 1.040000, 0.860000, 0.640000, 0.380000, 0.000000, 0.000000 } };



	public static double [][] getLookupMatrix(int cardinality){
		System.out.println("Getting matrix for cardinality::"+cardinality);
		switch(cardinality){
			case 2: return lookup_case2.clone(); 
			case 4: return lookup_case4.clone(); 
			case 8: return lookup_case8.clone(); 
			case 16: return lookup_case16.clone();
		
		}
		System.out.println("No matrix");
		return null;
	}
	
	public static double [] getBreakpoints(int cardinality){
	
		switch(cardinality){
			case 2: return breakpoint_card_2; 
			case 4: return breakpoint_card_4; 
			case 8: return breakpoint_card_8; 						
		}
		System.out.println("No matrix");
		return null;
	
	}
	public static int log2(int bits){

		int log = 0;
	    if( ( bits & 0xffff0000 ) != 0 ) { bits >>>= 16; log = 16; }
	    if( bits >= 256 ) { bits >>>= 8; log += 8; }
	    if( bits >= 16  ) { bits >>>= 4; log += 4; }
	    if( bits >= 4   ) { bits >>>= 2; log += 2; }

	    return log + ( bits >>> 1 );
	}
}