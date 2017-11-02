/*package edu.uc.rphash.decoders;

public interface Decoder {
	abstract void setVariance(Float parameterObject);
	abstract int getDimensionality();
	abstract long[] decode(float[] f);
	abstract float getErrorRadius();
	abstract float getDistance();
	abstract boolean selfScaling();
}
*/


package edu.uc.rphash.decoders;

import edu.uc.rphash.frequentItemSet.Countable;

public interface Decoder {

	abstract int getDimensionality();
	abstract long[] decode(float[] f);
	abstract float getErrorRadius();
	abstract float getDistance();
	
	abstract boolean selfScaling();
//	abstract float[] getVariance();
//	abstract void setVariance(float[] parameterObject);
	void setCounter(Countable counter);
}
