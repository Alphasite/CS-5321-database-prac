package cs4321.project1;

import cs4321.project1.tree.*;

/**
 * Class for a parser that can parse a string and produce an expression tree. To
 * keep the code simple, this does no input checking whatsoever so it only works
 * on correct input.
 * <p>
 * An expression is one or more terms separated by + or - signs. A term is one
 * or more factors separated by * or / signs. A factor is an expression in
 * parentheses (), a factor with a unary - before it, or a number.
 *
 * @author Lucja Kot
 * @author Nishad Mathur (nm594), Antoine Klopocki (ajk332), Antoine Salon (ajs672)
 */
public class Parser {

    private String[] tokens;
    private int currentToken; // pointer to next input token to be processed

    /**
     * @precondition input represents a valid expression with all tokens
     * separated by spaces, e.g. "3.0 - ( 1.0 + 2.0 ) / - 5.0. All
     * tokens must be either numbers that parse to Double, or one
     * of the symbols +, -, /, *, ( or ), and all parentheses must
     * be matched and properly nested.
     *
     * @param input
     *              The string to be parsed
     */
    public Parser(String input) {
        this.tokens = input.split("\\s+");
        currentToken = 0;
    }

    /**
     * Parse the input and build the expression tree
     *
     * @return the (root node of) the resulting tree
     */
    public TreeNode parse() {
        return expression();
    }

    /**
     * Parse the remaining input as far as needed to get the next factor
     *
     * @return the (root node of) the resulting subtree
     */
    private TreeNode factor() {
        TreeNode result;

        if (tokens[currentToken].equals("(")) {
            currentToken = currentToken + 1;
            result = expression();
            currentToken = currentToken + 1;
            return result;
        }

        if (tokens[currentToken].equals("-")) {
            currentToken = currentToken + 1;
            result = new UnaryMinusTreeNode(factor());
            return result;
        } else {
            result = new LeafTreeNode(Double.parseDouble(tokens[currentToken]));
            currentToken = currentToken + 1;
            return result;
        }
    }

    /**
     * Parse the remaining input as far as needed to get the next term
     *
     * @return the (root node of) the resulting subtree
     */
    private TreeNode term() {
        TreeNode result = factor();
        while (currentToken != tokens.length) {

            if (tokens[currentToken].equals("*")) {
                currentToken = currentToken + 1;
                TreeNode result2 = factor();
                result = new MultiplicationTreeNode(result, result2);
            } else {
                if (tokens[currentToken].equals("/")) {
                    currentToken = currentToken + 1;
                    TreeNode result2 = factor();
                    result = new DivisionTreeNode(result, result2);
                } else {
                    break;
                }
            }
        }

        return result;

    }

    /**
     * Parse the remaining input as far as needed to get the next expression
     *
     * @return the (root node of) the resulting subtree
     */
    private TreeNode expression() {
        TreeNode result = term();

        while (currentToken != tokens.length) {
            if (tokens[currentToken].equals("+")) {
                currentToken = currentToken + 1;
                TreeNode result2 = term();
                result = new AdditionTreeNode(result, result2);
            } else {
                if (tokens[currentToken].equals("-")) {
                    currentToken = currentToken + 1;
                    TreeNode result2 = term();
                    result = new SubtractionTreeNode(result, result2);
                } else {
                    break;
                }
            }
        }

        return result;
    }
}
