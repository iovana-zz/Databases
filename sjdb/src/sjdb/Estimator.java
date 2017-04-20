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

	}

	public void visit(Product op) {
	}

	public void visit(Join op) {
	}
}
