package query;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SubSelect;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.HashMap;

public class BreakWhereBuilder implements ExpressionVisitor {
    HashMap<Table,Expression> hashSelection;
    HashMap<TableCouple, Expression> hashJoin;


    public BreakWhereBuilder() {
        this.hashSelection=new HashMap<>();
        this.hashJoin=new HashMap<>();

    }

    public HashMap<Table, Expression> getHashSelection(Expression expression) {
        expression.accept(this);
        return hashSelection;
    }

    public HashMap<TableCouple, Expression> getHashJoin(Expression expression) {
        expression.accept(this);
        return hashJoin;
    }

    @Override
    public void visit(Column column) {throw new NotImplementedException();}

    @Override
    public void visit(LongValue longValue) {throw new NotImplementedException();}

    @Override
    public void visit(Parenthesis parenthesis) {
        parenthesis.getExpression().accept(this);
    }

    @Override
    public void visit(Addition addition) {throw new NotImplementedException();}

    @Override
    public void visit(Division division) {throw new NotImplementedException();}

    @Override
    public void visit(Multiplication multiplication) {throw new NotImplementedException();}

    @Override
    public void visit(Subtraction subtraction) {throw new NotImplementedException();}

    @Override
    public void visit(AndExpression andExpression) {
        andExpression.getLeftExpression().accept(this);
        andExpression.getRightExpression().accept(this);
    }

    @Override
    public void visit(OrExpression orExpression) {throw new NotImplementedException();}

    @Override
    public void visit(EqualsTo equalsTo) {
        if (equalsTo.getLeftExpression()instanceof Column &&  equalsTo.getRightExpression()instanceof Column){
            Table table1= ((Column) equalsTo.getLeftExpression()).getTable();
            Table table2= ((Column) equalsTo.getRightExpression()).getTable();
            hashJoin.put(new TableCouple(table1,table2),equalsTo);
        }
        else{
            if (equalsTo.getLeftExpression()instanceof Column){
                Table table= ((Column) equalsTo.getLeftExpression()).getTable();
                hashSelection.put(table, equalsTo);
            }
            else{
                if (equalsTo.getRightExpression()instanceof Column){
                    Table table= ((Column) equalsTo.getRightExpression()).getTable();
                    hashSelection.put(table, equalsTo);
                }
                else throw new NotImplementedException();
            }
        }
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        if (greaterThan.getLeftExpression()instanceof Column &&  greaterThan.getRightExpression()instanceof Column){
            Table table1= ((Column) greaterThan.getLeftExpression()).getTable();
            Table table2= ((Column) greaterThan.getRightExpression()).getTable();
            hashJoin.put(new TableCouple(table1,table2),greaterThan);
        }
        else{
            if (greaterThan.getLeftExpression()instanceof Column){
                Table table= ((Column) greaterThan.getLeftExpression()).getTable();
                hashSelection.put(table, greaterThan);
            }
            else{
                if (greaterThan.getRightExpression()instanceof Column){
                    Table table= ((Column) greaterThan.getRightExpression()).getTable();
                    hashSelection.put(table, greaterThan);
                }
                else throw new NotImplementedException();
            }
        }
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        if (greaterThanEquals.getLeftExpression()instanceof Column &&  greaterThanEquals.getRightExpression()instanceof Column){
            Table table1= ((Column) greaterThanEquals.getLeftExpression()).getTable();
            Table table2= ((Column) greaterThanEquals.getRightExpression()).getTable();
            hashJoin.put(new TableCouple(table1,table2),greaterThanEquals);
        }
        else{
            if (greaterThanEquals.getLeftExpression()instanceof Column){
                Table table= ((Column) greaterThanEquals.getLeftExpression()).getTable();
                hashSelection.put(table, greaterThanEquals);
            }
            else{
                if (greaterThanEquals.getRightExpression()instanceof Column){
                    Table table= ((Column) greaterThanEquals.getRightExpression()).getTable();
                    hashSelection.put(table, greaterThanEquals);
                }
                else throw new NotImplementedException();
            }
        }
    }

    @Override
    public void visit(MinorThan minorThan) {
        if (minorThan.getLeftExpression()instanceof Column &&  minorThan.getRightExpression()instanceof Column){
            Table table1= ((Column) minorThan.getLeftExpression()).getTable();
            Table table2= ((Column) minorThan.getRightExpression()).getTable();
            hashJoin.put(new TableCouple(table1,table2),minorThan);
        }
        else{
            if (minorThan.getLeftExpression()instanceof Column){
                Table table= ((Column) minorThan.getLeftExpression()).getTable();
                hashSelection.put(table, minorThan);
            }
            else{
                if (minorThan.getRightExpression()instanceof Column){
                    Table table= ((Column) minorThan.getRightExpression()).getTable();
                    hashSelection.put(table, minorThan);
                }
                else throw new NotImplementedException();
            }
        }
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        if (minorThanEquals.getLeftExpression()instanceof Column &&  minorThanEquals.getRightExpression()instanceof Column){
            Table table1= ((Column) minorThanEquals.getLeftExpression()).getTable();
            Table table2= ((Column) minorThanEquals.getRightExpression()).getTable();
            hashJoin.put(new TableCouple(table1,table2),minorThanEquals);
        }
        else{
            if (minorThanEquals.getLeftExpression()instanceof Column){
                Table table= ((Column) minorThanEquals.getLeftExpression()).getTable();
                hashSelection.put(table, minorThanEquals);
            }
            else{
                if (minorThanEquals.getRightExpression()instanceof Column){
                    Table table= ((Column) minorThanEquals.getRightExpression()).getTable();
                    hashSelection.put(table, minorThanEquals);
                }
                else throw new NotImplementedException();
            }
        }
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        if (notEqualsTo.getLeftExpression()instanceof Column &&  notEqualsTo.getRightExpression()instanceof Column){
            Table table1= ((Column) notEqualsTo.getLeftExpression()).getTable();
            Table table2= ((Column) notEqualsTo.getRightExpression()).getTable();
            hashJoin.put(new TableCouple(table1,table2),notEqualsTo);
        }
        else{
            if (notEqualsTo.getLeftExpression()instanceof Column){
                Table table= ((Column) notEqualsTo.getLeftExpression()).getTable();
                hashSelection.put(table, notEqualsTo);
            }
            else{
                if (notEqualsTo.getRightExpression()instanceof Column){
                    Table table= ((Column) notEqualsTo.getRightExpression()).getTable();
                    hashSelection.put(table, notEqualsTo);
                }
                else throw new NotImplementedException();
            }
        }
    }

    @Override
    public void visit(Between between) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(InExpression inExpression) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(NullValue nullValue) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(Function function) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(InverseExpression inverseExpression) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(DoubleValue doubleValue) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(StringValue stringValue) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(DateValue dateValue) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(TimeValue timeValue) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(TimestampValue timestampValue) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(SubSelect subSelect) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(CaseExpression caseExpression) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(WhenClause whenClause) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(Concat concat) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(Matches matches) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        throw new NotImplementedException();
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        throw new NotImplementedException();
    }
}
