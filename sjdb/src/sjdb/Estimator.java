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
	public void visit(Project op) {
		List<Attribute> attributes = op.getAttributes();
		Operator rel = op.getInput();
		Relation input = rel.getOutput();
		Relation output = new Relation(input.getTupleCount());

		List<Attribute> attr2 = input.getAttributes();
		for (int i = 0; i < attributes.size(); i++) {
			Attribute attr = new Attribute(findAttribute(attributes.get(i), attr2));
			if (attr != null) {
				output.addAttribute(attr);
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

		// loop through the attribute list and find the value count for the left attribute
		List<Attribute> attributes = input.getAttributes();
		Attribute left_attr = pred.getLeftAttribute();
		if(findAttribute(left_attr, attributes)!=null) {
			left_value_count = findAttribute(left_attr, attributes).getValueCount();
		}

		// true if the predicate is of the form attr=value
		if (pred.equalsValue()) {
			String value = pred.getRightValue();
			rel_count = input.getTupleCount()/left_value_count;
			output = new Relation((int)rel_count);
		} else {
			Attribute right_attr = pred.getRightAttribute();
			right_value_count = findAttribute(right_attr, attributes).getValueCount();
			float max_value = Math.max(left_value_count, right_value_count);
			output = new Relation(0);
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
	}

	public void visit(Join op) {
	}
}
