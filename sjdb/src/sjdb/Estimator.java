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
		// find the attributes of the projection
		List<Attribute> attributes = op.getAttributes();
		Operator rel = op.getInput();
		Relation input = rel.getOutput();
		// create new relation with the same tuple count as the input relation
		Relation output = new Relation(input.getTupleCount());

		// find the attributes and insert them in the new relation
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
		Attribute right_attr, attr1, attr2;

		//System.out.println("LEFT ATTR IS: " + left_attr);
		System.out.println("PRED IS: " + pred);

		// loop through the attribute list and find the value count for the left
		// attribute
		List<Attribute> attributes = input.getAttributes();
		left_attr = findAttribute(left_attr, attributes);

//		for(int i=0; i < attributes.size(); i++) {
////			System.out.println(attributes.get(i));
//		}
		//	System.out.println(left_attr);
		if (left_attr != null) {
			left_value_count = left_attr.getValueCount();
		}

		// true if the predicate is of the form attr=value
		if (pred.equalsValue()) {
			String value = pred.getRightValue();
			rel_count = input.getTupleCount() / left_value_count;
			output = new Relation((int) rel_count);
			attr1 = new Attribute(left_attr.getName(), 1);
			output.addAttribute(attr1);
		} else {
			right_attr = pred.getRightAttribute();
			right_attr = findAttribute(right_attr, attributes);
			if (right_attr != null) {
				right_value_count = right_attr.getValueCount();
				float max_value = Math.max(left_value_count, right_value_count);
				rel_count = input.getTupleCount() / max_value;
				output = new Relation((int) rel_count);
				float min_value = Math.min(left_value_count, right_value_count);
				attr1 = new Attribute(left_attr.getName(), (int) min_value);
				attr2 = new Attribute(right_attr.getName(), (int) min_value);
				output.addAttribute(attr1);
				output.addAttribute(attr2);
			} else {
				output = new Relation(0);
			}
		}

		op.setOutput(output);
		(new Inspector()).visit(op);
	}

	// returns an attribute if found in the list
	private Attribute findAttribute(Attribute attr, List<Attribute> attributes) {
		for (int i = 0; i < attributes.size(); i++) {
			if (attributes.get(i).equals(attr)) {
				return attributes.get(i);
			}
		}
		return null;
	}

	public void visit(Product op) {
		// get left and right operators
		Operator left_op = op.getLeft();
		Operator right_op = op.getRight();

		// calculate the tuple count of the output relation
		float left_tuple_count = left_op.getOutput().getTupleCount();
		float right_tuple_count = right_op.getOutput().getTupleCount();
		float tuple_count = left_tuple_count * right_tuple_count;

		// create new relation
		Relation output = new Relation((int) tuple_count);
		List<Attribute> left_attr = left_op.getOutput().getAttributes();
		List<Attribute> right_attr = right_op.getOutput().getAttributes();

		// and add the attributes
		for (int i = 0; i < left_attr.size(); i++) {
			output.addAttribute(left_attr.get(i));
		}
		for (int i = 0; i < right_attr.size(); i++) {
			output.addAttribute(right_attr.get(i));
		}
		op.setOutput(output);
		(new Inspector()).visit(op);
	}

	public void visit(Join op) {
		Operator left_op = op.getLeft();
		Operator right_op = op.getRight();
		Predicate pred = op.getPredicate();

		// tuple count = T(R)T(S)/max(V(R,A),V(S,B))
		System.out.println(left_op);
		System.out.println(right_op);
		System.out.println(pred);

		Relation output = new Relation(0);
		op.setOutput(output);
		//	(new Inspector()).visit(op);
	}
}
