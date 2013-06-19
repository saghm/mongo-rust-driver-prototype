#[link(name="stream", vers="0.2", author="austin.estep@10gen.com")];
#[crate_type="lib"];

use std::iterator::IteratorUtil;

trait Stream<T:Eq> {
	//Return if the stream has more values.	
	fn has_next(& self) -> bool;
	//Get a borrowed pointer to the first element of the stream.
	fn first<'a>(&'a self) -> &'a T;	
	//Move the stream forward by count units.		
	fn pass(&mut self, count: int);
	//Apply a function to the first count units and return the results in a vector.
	fn process<V: Copy>(&mut self, count: int, f: &fn(&T) -> V) -> ~[V];
	//Collect the first count elements and return them in a vector.
	//This is logically equivalent to self.process(count, id), modulo pointer types.
	fn aggregate(&mut self, count: int) -> ~[T];
	//Aggregate elements of the stream until the head of the stream meets the predicate.
	fn until(&mut self, f: &fn(&T) -> bool) -> ~[T];
	//Look for the elements of search in the first element of the stream.
	//If the first element of the stream matches any element, return the first match.	
	fn expect(&self, search: &~[T]) -> Option<T>; 
	//Skip values which match the listed values until a different one is found.
	fn pass_while(&mut self, to_skip: &~[T]);
}

impl<T:Eq + Copy> Stream<T> for ~[T] {
	fn has_next(&self) -> bool {
		self.len() >= 1	
	}
	fn first<'a>(&'a self) -> &'a T {
		if self.is_empty() {
			fail!("cannot get the first element of an empty stream!");
		}
		&'a self[0]
	}
	fn pass(&mut self, count: int) {
		self.process(count, |&x| x);
	}

	fn process<V: Copy>(&mut self, count: int, f: &fn(&T) -> V) -> ~[V] {
		let mut c = 0;
		let mut ret: ~[V] = ~[];
		if !self.has_next() || count > self.len() as int {
			fail!("cannot process past end of stream!");
		}
		while self.has_next() && c < count {
			ret += [f(&self[0])];
			self.shift();
			c += 1;
		}
		ret
	}

	fn aggregate(&mut self, count: int) -> ~[T] {	
		self.process(count, |&x| x)
	}	
	
	fn until(&mut self, f: &fn(&T) -> bool) -> ~[T] {
		let mut ret: ~[T] = ~[];
		loop {
			if !self.has_next() || f(self.first()) {
				return ret;
			}
			ret.push(copy self[0]);
			self.pass(1);
		}
	}
	fn expect(&self, search: &~[T]) -> Option<T> {
		if !self.has_next() { return None; }
		for search.iter().advance |&choice| {
			if choice == self[0] { 
				return Some(choice); 
			}
		}
		None
	}
	fn pass_while(&mut self, to_skip: &~[T]) {
		while self.has_next() {
			let v = self.expect(to_skip);
			if v == None { return; }
			self.pass(1);
		}
	}	
}

#[cfg(test)]
mod tests {
	#[test]
	fn test_has_next() {
		let empty: ~[~str] = ~ [];
		assert_eq!(empty.has_next(), false);	
		assert_eq!((~[0,1,2]).has_next(), true);
	}

	#[test]
	fn test_first() {
		let full = ~[0];
		assert_eq!(full.first(), &0);
	}
	
	#[test]
	#[should_fail]
	fn test_first_fail() {
		let empty: ~[~str] = ~[];
		empty.first();
	}	
	#[test]
	fn test_pass() {
		let mut stream = ~[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15];
		stream.pass(3);
		assert_eq!(stream[0], 3);
		stream.pass(3);
		assert_eq!(stream[0], 6);
		stream.pass(6);
		assert_eq!(stream[0], 12);
		stream.pass(0);
		assert_eq!(stream[0], 12);
	}

	#[test]
	#[should_fail]
	fn test_pass_fail() {
		let mut stream = ~[0,1];
		stream.pass(1);
		assert_eq!(stream[0], 1);
		stream.pass(2);
	}

	#[test]
	#[should_fail]
	fn test_pass_runover() {
		let mut stream= ~[0,1];
		stream.pass(3);
	}

	#[test]
	fn test_process() {
		let mut stream = ~[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15];
		let f: &fn(&int) -> int = |&val| 2 * val;
		assert_eq!(stream.process(3, f), ~[0,2,4]);
		assert_eq!(stream.process(3, f), ~[6,8,10]);
	}

	#[test]
	#[should_fail]
	fn test_process_fail() {
		let mut stream = ~[0,1];
		let f: &fn(&int) -> int = |&val| 2 * val;
		assert_eq!(stream.process(1, f), ~[0]);
		stream.process(2, f);
	}

	#[test]
	#[should_fail]
	fn test_process_runover() {
		let mut stream = ~[0,1];
		let f: &fn(&int) -> int = |&val| 2 * val;
		stream.process(3, f);
	}
	#[test]
	fn test_aggregate() {
		let mut stream = ~[0,1,2,3,4,5,6,7,8,9];
		assert_eq!(stream.aggregate(3), ~[0,1,2]);
		assert_eq!(stream.aggregate(3), ~[3,4,5]);
	}

	#[test]
	fn test_until() {
		let mut stream = ~[0,1,2,3,4,5,6,7,8,9];
		let is_4: &fn(&int) -> bool = |&x| x == 4;
		assert_eq!(stream.until(is_4), ~[0,1,2,3]);
		let is_9: &fn(&int) -> bool = |&x| x == 9;
		assert_eq!(stream.until(is_9), ~[4,5,6,7,8]);
	}

	#[test]
	fn test_until_runover() {
		let mut stream = ~[0,1,2,3,4,5,6,7,8,9];
		let is_50: &fn(&int) -> bool = |&x| x == 50;
		assert_eq!(stream.until(is_50), ~[0,1,2,3,4,5,6,7,8,9]);
	}
	#[test]
	fn test_expect() {
		let stream = ~[0,1,2];
		assert_eq!(stream.expect(&~[0,1]), Some(0));
		assert_eq!(stream.expect(&~[1,0]), Some(0));
		assert_eq!(stream.expect(&~[3,4]), None);
	}

	#[test]
	fn test_pass_while() {
		let mut stream = ~[0,1,2,3,2,3,4,3,4,5];
		let to_skip = ~[1,2,3];
		stream.pass(1);
		stream.pass_while(&to_skip);
		assert_eq!(stream[0], 4);
		stream.pass(1);
		assert_eq!(stream[0], 3);	
	}

}
