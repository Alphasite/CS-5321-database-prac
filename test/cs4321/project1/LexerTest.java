package cs4321.project1;

import cs4321.project1.tokenizer.tokens.*;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by nishadmathur on 24/8/17.
 */
public class LexerTest {

    public static final double EPSILON = 0.0001;

    @Test
    public void constructor() throws Exception {
        new Lexer("");
    }

    @Test
    public void lexNumber() throws Exception {
        List<Token> tokens;

        tokens = new Lexer("1").lex();
        assertEquals(NumberToken.class, tokens.get(0).getClass());
        assertEquals(((NumberToken) tokens.get(0)).value, 1, EPSILON);

        tokens = new Lexer("3.").lex();
        assertEquals(NumberToken.class, tokens.get(0).getClass());
        assertEquals(((NumberToken) tokens.get(0)).value, 3, EPSILON);

        tokens = new Lexer("4.43").lex();
        assertEquals(NumberToken.class, tokens.get(0).getClass());
        assertEquals(((NumberToken) tokens.get(0)).value, 4.43, EPSILON);
    }

    @Test
    public void lexSimpleAddition() throws Exception {
        List<Token> tokens = new Lexer("2 + 3").lex();

        assertEquals(NumberToken.class, tokens.get(0).getClass());
        assertEquals(((NumberToken) tokens.get(0)).value, 2, EPSILON);

        assertEquals(AddToken.class, tokens.get(1).getClass());

        assertEquals(NumberToken.class, tokens.get(2).getClass());
        assertEquals(((NumberToken) tokens.get(2)).value, 3, EPSILON);
    }

    @Test
    public void lexSimpleSubtraction() throws Exception {
        List<Token> tokens = new Lexer("2 - 3").lex();

        assertEquals(NumberToken.class, tokens.get(0).getClass());
        assertEquals(((NumberToken) tokens.get(0)).value, 2, EPSILON);

        assertEquals(SubtractToken.class, tokens.get(1).getClass());

        assertEquals(NumberToken.class, tokens.get(2).getClass());
        assertEquals(((NumberToken) tokens.get(2)).value, 3, EPSILON);
    }

    @Test
    public void lexSimpleMultiplication() throws Exception {
        List<Token> tokens = new Lexer("2. * 3.0").lex();

        assertEquals(NumberToken.class, tokens.get(0).getClass());
        assertEquals(((NumberToken) tokens.get(0)).value, 2, EPSILON);

        assertEquals(MultiplyToken.class, tokens.get(1).getClass());

        assertEquals(NumberToken.class, tokens.get(2).getClass());
        assertEquals(((NumberToken) tokens.get(2)).value, 3, EPSILON);
    }

    @Test
    public void lexSimpleDivision() throws Exception {
        List<Token> tokens = new Lexer("2 / 3").lex();

        assertEquals(NumberToken.class, tokens.get(0).getClass());
        assertEquals(((NumberToken) tokens.get(0)).value, 2, EPSILON);

        assertEquals(DivideToken.class, tokens.get(1).getClass());

        assertEquals(NumberToken.class, tokens.get(2).getClass());
        assertEquals(((NumberToken) tokens.get(2)).value, 3, EPSILON);
    }

    @Test
    public void lexBracketedNumber() throws Exception {
        List<Token> tokens = new Lexer("(2)").lex();

        assertEquals(LeftBracketToken.class, tokens.get(0).getClass());

        assertEquals(NumberToken.class, tokens.get(1).getClass());

        assertEquals(2, ((NumberToken) tokens.get(1)).value, EPSILON);
        assertEquals(RightBracketToken.class, tokens.get(2).getClass());
    }

    @Test
    public void lexBracketedExpression() throws Exception {
        List<Token> tokens = new Lexer("(2 / 3)").lex();

        assertEquals(LeftBracketToken.class, tokens.get(0).getClass());

        assertEquals(NumberToken.class, tokens.get(1).getClass());
        assertEquals(((NumberToken) tokens.get(1)).value, 2, EPSILON);

        assertEquals(DivideToken.class, tokens.get(2).getClass());

        assertEquals(NumberToken.class, tokens.get(3).getClass());
        assertEquals(((NumberToken) tokens.get(3)).value, 3, EPSILON);

        assertEquals(RightBracketToken.class, tokens.get(4).getClass());
    }

    @Test
    public void lexBracketedExpressionCompound() throws Exception {
        List<Token> tokens = new Lexer("(2 / 3.8) + 4.0").lex();

        assertEquals(LeftBracketToken.class, tokens.get(0).getClass());

        assertEquals(NumberToken.class, tokens.get(1).getClass());
        assertEquals(((NumberToken) tokens.get(1)).value, 2, EPSILON);

        assertEquals(DivideToken.class, tokens.get(2).getClass());

        assertEquals(NumberToken.class, tokens.get(3).getClass());
        assertEquals(((NumberToken) tokens.get(3)).value, 3.8, EPSILON);

        assertEquals(RightBracketToken.class, tokens.get(4).getClass());
        assertEquals(AddToken.class, tokens.get(5).getClass());

        assertEquals(NumberToken.class, tokens.get(6).getClass());
        assertEquals(((NumberToken) tokens.get(6)).value, 4, EPSILON);
    }
}
