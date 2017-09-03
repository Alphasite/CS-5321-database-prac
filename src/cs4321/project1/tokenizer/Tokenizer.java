package cs4321.project1.tokenizer;

import cs4321.project1.tokenizer.Tokens.*;
import cs4321.project1.Utilities.Pair;

/**
 * Created by nishadmathur on 23/8/17.
 */
public class Tokenizer {
    public final TokenType NUMBER;
    public final TokenType ADD;
    public final TokenType SUBTRACT;
    public final TokenType DIVIDE;
    public final TokenType MULTIPLY;
    public final TokenType LEFT_BRACKET;
    public final TokenType RIGHT_BRACKET;

    public Tokenizer(String expression) {
        this.NUMBER = new TokenType("\\G\\s*(\\d+(\\.\\d*)?)\\s*", expression);
        this.ADD = new TokenType("\\G\\s*(\\+)\\s*", expression);
        this.SUBTRACT = new TokenType("\\G\\s*(\\-)\\s*", expression);
        this.DIVIDE = new TokenType("\\G\\s*(/)\\s*", expression);
        this.MULTIPLY = new TokenType("\\G\\s*(\\*)\\s*", expression);
        this.LEFT_BRACKET = new TokenType("\\G\\s*(\\()\\s*", expression);
        this.RIGHT_BRACKET = new TokenType("\\G\\s*(\\))\\s*", expression);
    }

    public Pair<Token, Integer> tokenize(int index) {
        if (this.NUMBER.matcher.find(index)) {
            String number = this.NUMBER.matcher.group(1);
            return new Pair<>(
                    new NumberToken(Double.parseDouble(number)),
                    this.NUMBER.matcher.end()
            );
        }

        if (this.ADD.matcher.find(index)) {
            return new Pair<>(
                    new AddToken(),
                    this.ADD.matcher.end()
            );
        }

        if (this.SUBTRACT.matcher.find(index)) {
            return new Pair<>(
                    new SubtractToken(),
                    this.SUBTRACT.matcher.end()
            );
        }

        if (this.DIVIDE.matcher.find(index)) {
            return new Pair<>(
                    new DivideToken(),
                    this.DIVIDE.matcher.end()
            );
        }

        if (this.MULTIPLY.matcher.find(index)) {
            return new Pair<>(
                    new MultiplyToken(),
                    this.MULTIPLY.matcher.end()
            );
        }

        if (this.LEFT_BRACKET.matcher.find(index)) {
            return new Pair<>(
                    new LeftBracketToken(),
                    this.LEFT_BRACKET.matcher.end()
            );
        }

        if (this.RIGHT_BRACKET.matcher.find(index)) {
            return new Pair<>(
                    new RightBracketToken(),
                    this.RIGHT_BRACKET.matcher.end()
            );
        }

        return null;
    }
}

