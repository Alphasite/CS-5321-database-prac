package cs4321.project1;

import cs4321.project1.tokenizer.Tokens.*;
import cs4321.project1.tree.*;

import java.util.List;

/**
 * Class for a parser that can parse a string and produce an expression tree. To
 * keep the code simple, this does no input checking whatsoever so it only works
 * on correct input.
 * 
 * An expression is one or more terms separated by + or - signs. A term is one
 * or more factors separated by * or / signs. A factor is an expression in
 * parentheses (), a factor with a unary - before it, or a number.
 * 
 * @author Lucja Kot
 * @author Your names and netids go here
 */
public class Parser {

	private List<Token> tokens;
	private int currentToken; // pointer to next input token to be processed

	/**
	 * @precondition input represents a valid expression with all tokens
	 *               separated by spaces, e.g. "3.0 - ( 1.0 + 2.0 ) / - 5.0. All
	 *               tokens must be either numbers that parse to Double, or one
	 *               of the symbols +, -, /, *, ( or ), and all parentheses must
	 *               be matched and properly nested.
	 */
	public Parser(String input) {
		Lexer lexer = new Lexer(input);
		this.tokens = lexer.lex();
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

		// TODO fill me in
		if (this.currentToken < this.tokens.size()) {
			Token token = this.tokens.get(this.currentToken);

			if (token instanceof NumberToken) {
				NumberToken numberToken = (NumberToken) token;
				this.currentToken += 1;
				return new LeafTreeNode(numberToken.value);
			}
		}

		if (this.currentToken + 1 < this.tokens.size()) {
			Token token = this.tokens.get(this.currentToken);

			if (token instanceof SubtractToken) {
				this.currentToken += 1;

				TreeNode rightHandNode = this.factor();

				if (rightHandNode != null) {
					return new UnaryMinusTreeNode(rightHandNode);
				}
			}
		}

		if (this.currentToken + 2 < this.tokens.size()) {
			Token token = this.tokens.get(this.currentToken);

			if (token instanceof LeftBracketToken) {
				this.currentToken += 1;

				TreeNode expression = this.expression();

				if (expression != null) {
					// safe to assume that left bracket must also be present.
					this.currentToken += 1;

					return expression;
				}
			}
		}

		return null;
	}

	/**
	 * Parse the remaining input as far as needed to get the next term
	 * 
	 * @return the (root node of) the resulting subtree
	 */
	private TreeNode term() {
		TreeNode lhs = this.factor();

		// TODO fill me in
		while (this.currentToken < this.tokens.size()) {
			if (lhs != null) {
				Token token = this.tokens.get(this.currentToken);
				if (token instanceof MultiplyToken) {
					this.currentToken += 1;
					TreeNode rhs = this.factor();
					lhs = new MultiplicationTreeNode(lhs, rhs);
					continue;
				}

				if (token instanceof DivideToken) {
					this.currentToken += 1;
					TreeNode rhs = this.factor();
					lhs = new DivisionTreeNode(lhs, rhs);
					continue;
				}

				break;
			} else {
				break;
			}
		}

		return lhs;
	}

	/**
	 * Parse the remaining input as far as needed to get the next expression
	 * 
	 * @return the (root node of) the resulting subtree
	 */
	private TreeNode expression() {
		TreeNode lhs = this.term();

		// TODO fill me in
		while (this.currentToken < this.tokens.size()) {
			if (lhs != null) {
				Token token = this.tokens.get(this.currentToken);
				if (token instanceof AddToken) {
					this.currentToken += 1;
					TreeNode rhs = this.term();
					lhs = new AdditionTreeNode(lhs, rhs);
					continue;
				}

				if (token instanceof SubtractToken) {
					this.currentToken += 1;
					TreeNode rhs = this.term();
					lhs = new SubtractionTreeNode(lhs, rhs);
					continue;
				}

				break;
			} else {
				break;
			}
		}
		return lhs;

	}
}
