package hw1;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

public class Query {
	private String q;
	
	public Query(String q) {
		this.q = q;
	}
	
	public Relation execute()  {
		Statement statement = null;
		try {
			statement = CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
			System.out.println("Unable to parse query");
			e.printStackTrace();
		} 
		
		Select selectStatement = (Select) statement;
		PlainSelect sb = (PlainSelect)selectStatement.getSelectBody();
		
		List<SelectItem> selectItems = sb.getSelectItems();
		List<Join> joins = sb.getJoins();
		Catalog catalog = Database.getCatalog();
		FromItem fromItem = sb.getFromItem();
		int tableID = catalog.getTableId(fromItem.toString());
		TupleDesc tupleDesc = catalog.getTupleDesc(tableID);
		ArrayList<Tuple> tuples = catalog.getDbFile(tableID).getAllTuples();
		Relation relation = new Relation(tuples,tupleDesc);
		
		//Join
		if (joins != null) {
			for(Join join : joins) {
				FromItem rightItem = join.getRightItem();
				int rightID = catalog.getTableId(rightItem.toString());
				ArrayList<Tuple> rightTuples = catalog.getDbFile(rightID).getAllTuples();
				TupleDesc rightTupleDesc = catalog.getTupleDesc(rightID);
				Relation rightRelation = new Relation(rightTuples, rightTupleDesc);
				
				String str[] = join.getOnExpression().toString().split(" = ");
				String leftStr[] = str[0].split("\\.");
				String rightStr[] = str[1].split("\\.");
				int left = -1;
				int right = -1;
				
				for(int i = 0; i < relation.getDesc().numFields(); i++) {
					if(relation.getDesc().getFieldName(i).equalsIgnoreCase(leftStr[1])) {
						left=i;
					}
					if(relation.getDesc().getFieldName(i).equalsIgnoreCase(rightStr[1])) {
						left=i;
					}
				} 
				for(int i = 0; i < rightTupleDesc.numFields(); i++) {
					if(rightTupleDesc.getFieldName(i).equalsIgnoreCase(leftStr[1])) {
						right=i;
					}
					if(rightTupleDesc.getFieldName(i).equalsIgnoreCase(rightStr[1])) {
						right=i;
					}
				}
				relation = relation.join(rightRelation, left, right);
			}
		}
		
		//Where
		WhereExpressionVisitor whereVisitor = new WhereExpressionVisitor();
		Expression where = sb.getWhere();
		if(where != null) {
			where.accept(whereVisitor);
			relation = relation.select(tupleDesc.nameToId(whereVisitor.getLeft()), whereVisitor.getOp(), whereVisitor.getRight());
		}
		
		//Project
		ArrayList<Integer> columnIDs = new ArrayList<>();
		
		//Handle Select All (*)
		if(!selectItems.get(0).toString().equals("*")) {
			if(selectItems.get(0).toString().equals("COUNT(*)")) {
				columnIDs.add(0);
				relation = relation.project(columnIDs);
				relation = relation.aggregate(AggregateOperator.COUNT, false);
				return relation;
			} else {			
				for(SelectItem selectitem : selectItems) {
					ColumnVisitor columnVisitor = new ColumnVisitor();
					selectitem.accept(columnVisitor);	
					for(int i = 0; i < relation.getDesc().numFields(); i++) {
						 if(relation.getDesc().getFieldName(i).equals(columnVisitor.getColumn())) {
							 columnIDs.add(i);
						 }
					 }
				}
				relation = relation.project(columnIDs);
				
				for(SelectItem selectitem : selectItems) {
					ColumnVisitor columnVisitor = new ColumnVisitor();
					selectitem.accept(columnVisitor);
					List<Expression> groupBy = sb.getGroupByColumnReferences();
					if(!columnVisitor.isAggregate()) {
					}
					else {				
						if(groupBy != null) {
							relation = relation.aggregate(columnVisitor.getOp(), true);
						}
						else {
							relation = relation.aggregate(columnVisitor.getOp(), false);
						}
					}
				}
			}	
		}

		return relation;
		
		}
	}
