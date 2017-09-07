package cs4321.project1.tokenizer.Tokens;

/**
 * Created by nishadmathur on 23/8/17.
 */
public class NumberToken implements Token {
    public final double value;

    public NumberToken(double value) {
        this.value = value;
    }
}
