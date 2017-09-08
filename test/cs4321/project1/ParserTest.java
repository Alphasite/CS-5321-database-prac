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

    //ADDED TEST, testing brackets
    @Test
    public void testBracketSimple() {
        Parser p1 = new Parser("( 1.0 )");
        TreeNode parseResult1 = p1.parse();
        PrintTreeVisitor v1 = new PrintTreeVisitor();
        parseResult1.accept(v1);
        assertEquals("1.0", v1.getResult());
    }

    //ADDED TEST, testing a simple addition
    @Test
    public void testAdditionSimple() {
        Parser p1 = new Parser("1.0 + 2.0");
        TreeNode parseResult1 = p1.parse();
        PrintTreeVisitor v1 = new PrintTreeVisitor();
        parseResult1.accept(v1);
        assertEquals("(1.0+2.0)", v1.getResult());
    }

    //ADDED TEST, testing a simple subtraction
    @Test
    public void testSubtractionSimple() {
        Parser p1 = new Parser("3.0 - 1.0");
        TreeNode parseResult1 = p1.parse();
        PrintTreeVisitor v1 = new PrintTreeVisitor();
        parseResult1.accept(v1);
        assertEquals("(3.0-1.0)", v1.getResult());
    }

    //ADDED TEST, testing a simple multiplication
    @Test
    public void testMultiplicationSimple() {
        Parser p1 = new Parser("2.0 * 1.0");
        TreeNode parseResult1 = p1.parse();
        PrintTreeVisitor v1 = new PrintTreeVisitor();
        parseResult1.accept(v1);
        assertEquals("(2.0*1.0)", v1.getResult());
    }

    //ADDED TEST, testing a simple division
    @Test
    public void testDivisionSimple() {
        Parser p1 = new Parser("2.0 / 3.0");
        TreeNode parseResult1 = p1.parse();
        PrintTreeVisitor v1 = new PrintTreeVisitor();
        parseResult1.accept(v1);
        assertEquals("(2.0/3.0)", v1.getResult());
    }

    //ADDED TEST, flat expression with multiple operations
    @Test
    public void testFlatExpressionSimple() {
        Parser p1 = new Parser("2.0 + 3.0 - 1.0 + 2.0 - 3.0");
        TreeNode n = p1.parse();
        PrintTreeVisitor v1 = new PrintTreeVisitor();
        n.accept(v1);
        assertEquals("((((2.0+3.0)-1.0)+2.0)-3.0)", v1.getResult());
    }

    //ADDED TEST, testing a compounded expression
    @Test
    public void testCompoundSimpleRight() {
        Parser p1 = new Parser("( 1.0 / 3.0 ) + 2.0");
        TreeNode parseResult1 = p1.parse();
        PrintTreeVisitor v1 = new PrintTreeVisitor();
        parseResult1.accept(v1);
        assertEquals("((1.0/3.0)+2.0)", v1.getResult());
    }

    //ADDED TEST, testing another compounded expression
    @Test
    public void testCompoundSimpleLeft() {
        Parser p1 = new Parser("2.0 / ( 1.0 - 3.0 )");
        TreeNode parseResult1 = p1.parse();
        PrintTreeVisitor v1 = new PrintTreeVisitor();
        parseResult1.accept(v1);
        assertEquals("(2.0/(1.0-3.0))", v1.getResult());
    }

    //ADDED TEST, testing another compounded expression, without bracket
    @Test
    public void testCompoundSimpleNoBracketDivAdd() {
        Parser p1 = new Parser("1.0 / 3.0 + 2.0");
        TreeNode parseResult1 = p1.parse();
        PrintTreeVisitor v1 = new PrintTreeVisitor();
        parseResult1.accept(v1);
        assertEquals("((1.0/3.0)+2.0)", v1.getResult());
    }

    //ADDED TEST, testing another compounded expression, without bracket
    @Test
    public void testCompoundSimpleNoBracketAddDiv() {
        Parser p1 = new Parser("2.0 + 1.0 / 3.0");
        TreeNode parseResult1 = p1.parse();
        PrintTreeVisitor v1 = new PrintTreeVisitor();
        parseResult1.accept(v1);
        assertEquals("(2.0+(1.0/3.0))", v1.getResult());
    }

    //ADDED TEST, testing the example provided in the instructions of the assignment
    @Test
    public void testExampleProvidedInInstructions() {
        Parser p1 = new Parser("- 2.0 * ( 3.0 + 1.0 )");
        TreeNode parseResult1 = p1.parse();
        PrintTreeVisitor v1 = new PrintTreeVisitor();
        parseResult1.accept(v1);
        assertEquals("((-2.0)*(3.0+1.0))", v1.getResult());
    }

    //ADDED TEST, testing the example called "Example Complex A"
    @Test
    public void testExampleComplexA() {
        Parser p1 = new Parser("- ( - 5.0 ) * 3.0 - ( 1.0 + 4.0 / 2.0 )");
        TreeNode parseResult1 = p1.parse();
        PrintTreeVisitor v1 = new PrintTreeVisitor();
        parseResult1.accept(v1);
        assertEquals("(((-(-5.0))*3.0)-(1.0+(4.0/2.0)))", v1.getResult());
    }

    //ADDED TEST, throw everything at it
    @Test
    public void testNestedExpressionComplex() {
        Parser p1 = new Parser("( ( 4.0 + ( - 2.0 * 3.0 ) ) / - 2.0 + 5.0 )");
        TreeNode n = p1.parse();
        PrintTreeVisitor v1 = new PrintTreeVisitor();
        n.accept(v1);
        assertEquals("(((4.0+((-2.0)*3.0))/(-2.0))+5.0)", v1.getResult());
    }
}
