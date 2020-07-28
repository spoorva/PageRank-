package edu.coursera.distributed;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;


public class Test{
 public class Foo {
    
        String name;
        List<Rando> random = new ArrayList<Rando>();

        public Foo(String name) {
            this.name = name;
        }    
    }

public class Rando {

        String name;

        public Rando(String name) {
            this.name = name;
        }

        
    }

 List <Foo> foos = new ArrayList<>();
    
 
 //IntStream.
 //range(1, 5).forEach(i -> foos.add(new Foo("foo"+i)));


}