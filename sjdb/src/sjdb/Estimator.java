package sjdb;

import java.util.List;
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
		// get the predicate value
		Predicate pred = op.getPredicate();
		// get the input relation
		Relation input = op.getInput().getOutput();
		float left_value_count = 0;
		float right_value_count = 0;
		float rel_count = 0;
		Relation output;
		Attribute left_attr = pred.getLeftAttribute();
		Attribute right_attr, attr1, attr2;

		// loop through the attribute list and find the value count for the left attribute
		List<Attribute> attributes = input.getAttributes();
		if(findAttribute(left_attr, attributes)!=null) {
			left_attr = findAttribute(left_attr, attributes);
			left_value_count = left_attr.getValueCount();
		}

		// true if the predicate is of the form attr=value
		if (pred.equalsValue()) {
			// String value = pred.getRightValue();
			rel_count = input.getTupleCount()/left_value_count;
			output = new Relation((int)rel_count);
			//set the value count to 1
			attr1 = new Attribute(left_attr.getName(), 1);
			for(int i = 0; i < attributes.size(); i++) {
				if(!attributes.get(i).equals(left_attr)) {
					output.addAttribute(attributes.get(i));
				}
			}
			output.addAttribute(attr1);
		} else {
			// get the right attribute
			right_attr = pred.getRightAttribute();
			//find it in the relation
			right_attr = findAttribute(right_attr, attributes);
			//get the value count
			right_value_count = right_attr.getValueCount();
			float max_value = Math.max(left_value_count, right_value_count);
			rel_count = input.getTupleCount()/max_value;
			output = new Relation((int)rel_count);
			float min_value = Math.min(left_value_count, right_value_count);
			// check also if V(R, A) <= T(R)
			min_value = Math.min(rel_count, min_value);
			// create left and right attributes
			attr1 = new Attribute(left_attr.getName(), (int) min_value);
			attr2 = new Attribute(right_attr.getName(), (int) min_value);
			for(int i = 0; i < attributes.size(); i++) {
				if(!attributes.get(i).equals(left_attr) && !attributes.get(i).equals(right_attr)) {
					output.addAttribute(attributes.get(i));
				}
			}
			output.addAttribute(attr1);
			output.addAttribute(attr2);
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

		Relation left_rel = left_op.getOutput();
		Relation right_rel = right_op.getOutput();

		// calculate the tuple count of the output relation
		float left_tuple_count = left_rel.getTupleCount();
		float right_tuple_count = right_rel.getTupleCount();
		// T(R x S) = T(R)T(S)
		float tuple_count = left_tuple_count * right_tuple_count;

		// create new relation
		Relation output = new Relation((int) tuple_count);
		List<Attribute> left_attr = left_op.getOutput().getAttributes();
		List<Attribute> right_attr = right_op.getOutput().getAttributes();

		// and add the attributes
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
		Operator left_op = op.getLeft();
		Operator right_op = op.getRight();
		Predicate pred = op.getPredicate();

		Relation left_rel = left_op.getOutput();
		Relation right_rel = right_op.getOutput();

		// get tuple count of relation A and B
		float left_tuple_count = left_rel.getTupleCount();
		float right_tuple_count = right_rel.getTupleCount();

		// get the left and right attributes from the predicate
		Attribute left_attr = pred.getLeftAttribute();
		Attribute right_attr = pred.getRightAttribute();

		// get the attributes from relation A and B
		List<Attribute> left_attributes = left_rel.getAttributes();
		List<Attribute> right_attributes = right_rel.getAttributes();

		// find left_attr and right_attr in the relations and get their value count
		float left_value_count = findAttribute(left_attr, left_attributes).getValueCount();
		float right_value_count = findAttribute(right_attr, right_attributes).getValueCount();

		// tuple count = T(R)T(S)/max(V(R,A),V(S,B))
		float max_value = Math.max(left_value_count, right_value_count);
		float tuple_count = left_tuple_count * right_tuple_count/max_value;
		Relation output = new Relation((int) tuple_count);

		// value count = min(V(R, A), V(S, B))
		float min_value = Math.min(left_value_count, right_value_count);
		// check if value count is less than tuple count
		min_value = Math.min(tuple_count, min_value);
		// create the attributes with appropriate value counts
		Attribute attr1 = new Attribute(left_attr.getName(), (int) min_value);
		Attribute attr2 = new Attribute(right_attr.getName(), (int) min_value);

		// add the rest of the attributes to the relation
		for(int i = 0; i < left_attributes.size(); i++) {
			if(!left_attributes.get(i).equals(left_attr)){
				output.addAttribute(left_attributes.get(i));
			}
		}
		output.addAttribute(attr1);
		for(int i = 0; i < right_attributes.size(); i++) {
			if(!right_attributes.get(i).equals(right_attr)){
				output.addAttribute(left_attributes.get(i));
			}
		}
		output.addAttribute(attr2);

		op.setOutput(output);
		(new Inspector()).visit(op);
	}
}