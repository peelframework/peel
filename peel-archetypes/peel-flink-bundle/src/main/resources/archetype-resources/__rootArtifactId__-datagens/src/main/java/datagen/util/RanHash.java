#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.datagen.util;

/**
 * c.f. "Press, William H. Numerical recipes 3rd edition: The art of scientific computing. Cambridge
 * university press, 2007.", pp. 352
 */
public class RanHash implements SymmetricPRNG {

    private static final double D_2_POW_NEG_64 = 5.4210108624275221700e-20;

    private long seed;

    private long currentPos;

    public RanHash() {}

    public RanHash(long seed) {
        seed(seed);
    }

    @Override
    public void seed(long seed) {
        this.seed = seed;
    }

    @Override
    public void skipTo(long pos) {
        currentPos = pos;
    }

    @Override
    public double next() {

        long x = seed + currentPos;

        x = 3935559000370003845L * x + 2691343689449507681L;
        x = x ^ (x >> 21);
        x = x ^ (x << 37);
        x = x ^ (x >> 4);
        x = 4768777513237032717L * x;
        x = x ^ (x << 20);
        x = x ^ (x >> 41);
        x = x ^ (x << 5);

        currentPos++;

        return x * D_2_POW_NEG_64 + 0.5;
    }

    @Override
    public int nextInt(int k) {
        // omitting >= 0 check here
        return (int) Math.floor(next() * k);
    }
}
