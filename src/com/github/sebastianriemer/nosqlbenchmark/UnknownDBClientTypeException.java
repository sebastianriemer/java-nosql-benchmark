package com.github.sebastianriemer.nosqlbenchmark;


@SuppressWarnings("serial")
public class UnknownDBClientTypeException extends Exception {
	public UnknownDBClientTypeException(String message) { super(message); }
}
