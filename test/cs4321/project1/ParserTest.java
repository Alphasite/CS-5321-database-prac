package cs4321.project1;

import cs4321.project1.tree.TreeNode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ParserTest {

	/*
     * This class depends on the correct functioning of PrintTreeVisitor(), which is provided for you.
	 */

    @Test
    public void testSingleNumber() {
        Parser p1 = new Parser("1.0");
        TreeNode parseResult1 = p1.parse();
        PrintTreeVisitor v1 = new PrintTreeVisitor();
        parseResult1.accept(v1);
        assertEquals("1.0", v1.getResult());
    }

    @Test
    public void testUnaryMinusSimple() {
        Parser p1 = new Parser("- 1.0");
        TreeNode parseResult1 = p1.parse();
        PrintTreeVisitor v1 = new PrintTreeVisitor();
        parseResult1.accept(v1);
        assertEquals("(-1.0)", v1.getResult());
    }

    @Test
    public void testUnaryMinusComplex() {
        Parser p1 = new Parser("- - 1.0");
        TreeNode parseResult1 = p1.parse();
        PrintTreeVisitor v1 = new PrintTreeVisitor();
        parseResult1.accept(v1);
        assertEquals("(-(-1.0))", v1.getResult());
    }

    @Test
    public void testBracketSimple() {
        Parser p1 = new Parser("(1.0)");
        TreeNode parseResult1 = p1.parse();
        PrintTreeVisitor v1 = new PrintTreeVisitor();
        parseResult1.accept(v1);
        assertEquals("1.0", v1.getResult());
    }

    @Test
    public void testAdditionSimple() {
        Parser p1 = new Parser("1.0 + 1.0");
        TreeNode parseResult1 = p1.parse();
        PrintTreeVisitor v1 = new PrintTreeVisitor();
        parseResult1.accept(v1);
        assertEquals("(1.0+1.0)", v1.getResult());
    }

    @Test
    public void testSubtractionSimple() {
        Parser p1 = new Parser("1.0 - 1.0");
        TreeNode parseResult1 = p1.parse();
        PrintTreeVisitor v1 = new PrintTreeVisitor();
        parseResult1.accept(v1);
        assertEquals("(1.0-1.0)", v1.getResult());
    }

    @Test
    public void testMultiplicationSimple() {
        Parser p1 = new Parser("1.0 * 1.0");
        TreeNode parseResult1 = p1.parse();
        PrintTreeVisitor v1 = new PrintTreeVisitor();
        parseResult1.accept(v1);
        assertEquals("(1.0*1.0)", v1.getResult());
    }

    @Test
    public void testDivisionSimple() {
        Parser p1 = new Parser("1.0 / 1.0");
        TreeNode parseResult1 = p1.parse();
        PrintTreeVisitor v1 = new PrintTreeVisitor();
        parseResult1.accept(v1);
        assertEquals("(1.0/1.0)", v1.getResult());
    }

    @Test
    public void testCompoundSimpleRight() {
        Parser p1 = new Parser("(1.0 / 3.0) + 2.0");
        TreeNode parseResult1 = p1.parse();
        PrintTreeVisitor v1 = new PrintTreeVisitor();
        parseResult1.accept(v1);
        assertEquals("((1.0/3.0)+2.0)", v1.getResult());
    }

    @Test
    public void testCompoundSimpleLeft() {
        Parser p1 = new Parser("2.0 / (1.0 - 3.0)");
        TreeNode parseResult1 = p1.parse();
        PrintTreeVisitor v1 = new PrintTreeVisitor();
        parseResult1.accept(v1);
        assertEquals("(2.0/(1.0-3.0))", v1.getResult());
    }

    @Test
    public void testCompoundSimpleNoBracketDivAdd() {
        Parser p1 = new Parser("1.0 / 3.0 + 2.0");
        TreeNode parseResult1 = p1.parse();
        PrintTreeVisitor v1 = new PrintTreeVisitor();
        parseResult1.accept(v1);
        assertEquals("((1.0/3.0)+2.0)", v1.getResult());
    }

    @Test
    public void testCompoundSimpleNoBracketAddDiv() {
        Parser p1 = new Parser("2.0 + 1.0 / 3.0");
        TreeNode parseResult1 = p1.parse();
        PrintTreeVisitor v1 = new PrintTreeVisitor();
        parseResult1.accept(v1);
        assertEquals("(2.0+(1.0/3.0))", v1.getResult());
    }
}
