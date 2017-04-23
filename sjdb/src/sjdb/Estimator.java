package sjdb;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

public class Estimator implements PlanVisitor {

	public Estimator() {
		// empty constructor
	}

	/*
	 * Create output relation on Scan operator
	 *
	 * Example implementation of visit method for Scan operators.
	 */
	public void visit(Scan op) {
		Relation input = op.getRelation();
		Relation output = new Relation(input.getTupleCount());

		Iterator<Attribute> iter = input.getAttributes().iterator();
		while (iter.hasNext()) {
			output.addAttribute(new Attribute(iter.next()));
		}

		op.setOutput(output);
		(new Inspector()).visit(op);
	}

	//	public void visit(Project op) {
//		List<Attribute> attributes = op.getAttributes();
//		Operator rel = op.getInput();
//		Relation input = rel.getOutput();
//		Relation output = new Relation(input.getTupleCount());
//
//		List<Attribute> attr2 = input.getAttributes();
//		for (int i = 0; i < attributes.size(); i++) {
//			Attribute attr = new Attribute(findAttribute(attributes.get(i), attr2));
//			if (attr != null) {
//				output.addAttribute(attr);
//			}
//		}
//		op.setOutput(output);
//		(new Inspector()).visit(op);
//	}
	public void visit(Project op) {
		List<Attribute> attributes = op.getAttributes();
		Operator rel = op.getInput();
		Relation input = rel.getOutput();
		Relation output = new Relation(input.getTupleCount());

		List<Attribute> iter = input.getAttributes();
		for (int i = 0; i < attributes.size(); i++) {
			for (int j = 0; j < iter.size(); j++) {
				if ((attributes.get(i).getName()).equals(iter.get(j).getName())) {
					Attribute attr = new Attribute(iter.get(j));
					output.addAttribute(attr);
				}
			}
		}

		op.setOutput(output);
		(new Inspector()).visit(op);
	}

	public void visit(Select op) {
		Predicate pred = op.getPredicate();
		Relation input = op.getInput().getOutput();
		float left_value_count = 0;
		float right_value_count = 0;
		float rel_count = 0;
		Relation output;
		Attribute left_attr = pred.getLeftAttribute();
		Attribute right_attr;
		Attribute attr1;
		Attribute attr2;

		// loop through the attribute list and find the value count for the left attribute
		List<Attribute> attributes = input.getAttributes();
		if(findAttribute(left_attr, attributes)!=null) {
			left_value_count = findAttribute(left_attr, attributes).getValueCount();
		}

		// true if the predicate is of the form attr=value
		if (pred.equalsValue()) {
			String value = pred.getRightValue();
			rel_count = input.getTupleCount()/left_value_count;
			output = new Relation((int)rel_count);
			attr1 = new Attribute(left_attr.getName(), 1);
			output.addAttribute(attr1);
		} else {
			right_attr = pred.getRightAttribute();
			right_value_count = findAttribute(right_attr, attributes).getValueCount();
			float max_value = Math.max(left_value_count, right_value_count);
			rel_count = input.getTupleCount()/max_value;
			output = new Relation((int)rel_count);
			float min_value = Math.min(left_value_count, right_value_count);
			attr1 = new Attribute(left_attr.getName(), (int) min_value);
			attr2 = new Attribute(right_attr.getName(), (int) min_value);
			output.addAttribute(attr1);
			output.addAttribute(attr2);
		}

		op.setOutput(output);
		(new Inspector()).visit(op);
	}

	private Attribute findAttribute(Attribute attr, List<Attribute> attributes) {
		for (int i = 0; i < attributes.size(); i++) {
			if (attributes.get(i).equals(attr)) {
				return attributes.get(i);
			}
		}
		return null;
	}

	public void visit(Product op) {
		Operator left_op = op.getLeft();
		Operator right_op = op.getRight();

		float left_tuple_count = left_op.getOutput().getTupleCount();
		float right_tuple_count = right_op.getOutput().getTupleCount();
		float tuple_count = left_tuple_count * right_tuple_count;

		Relation output = new Relation((int) tuple_count);
		List<Attribute> left_attr = left_op.getOutput().getAttributes();
		List<Attribute> right_attr = right_op.getOutput().getAttributes();

		for(int i = 0; i <  left_attr.size(); i++) {
			output.addAttribute(left_attr.get(i));
		}
		for(int i = 0; i <  right_attr.size(); i++) {
			output.addAttribute(right_attr.get(i));
		}
		op.setOutput(output);
		(new Inspector()).visit(op);
	}

	public void visit(Join op) {
	}
}
