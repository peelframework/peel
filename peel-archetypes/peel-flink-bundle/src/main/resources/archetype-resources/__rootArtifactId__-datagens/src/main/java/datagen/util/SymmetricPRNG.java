#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.datagen.util;


public interface SymmetricPRNG {

    void seed(long seed);

    void skipTo(long pos);

    double next();

    int nextInt(int k);
}
