package org.dan.mystorm;


import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.*;

public class MyStorm {
    private String[] sentences = {
            "Spring Integration provides an extension of the Spring programming model to support the well-known Enterprise Integration Patterns.",
            "It enables lightweight messaging within Spring-based applications and supports integration with external systems via declarative adapters. ",
            " Those adapters provide a higher-level of abstraction over Spring’s support for remoting, messaging, and scheduling. ",
            "Spring Integration’s primary goal is to provide a simple model for building enterprise integration solutions while maintaining the separation of concerns that is essential for producing maintainable, testable code."
    };
    private Random random = new Random();

    private BlockingQueue sentenceQueue = new ArrayBlockingQueue(50000);
    private BlockingQueue wordQueue = new ArrayBlockingQueue(50000);

    private HashMap<String, Integer> wordCountMap = new HashMap<>();

    public void nextTuple() {
        int index =random.nextInt(sentences.length);
        String sentence = sentences[index];
        sentenceQueue.add(sentence);
    }

    public void split(String sentence) {
        System.out.println("receive sentence: " + sentence);
        String[] words = sentence.split(" ");
        for(String word : words) {
            wordQueue.add(word);
        }
    }

    public void count(String word) {
        if(!wordCountMap.containsKey(word)) {
            wordCountMap.put(word, 1);
        } else {
            int count = wordCountMap.get(word);
            wordCountMap.put(word, count++);
        }
        System.out.println("word count: " + wordCountMap);
    }


    public BlockingQueue getSentenceQueue() {
        return sentenceQueue;
    }

    public BlockingQueue getWordQueue() {
        return wordQueue;
    }

    public HashMap<String, Integer> getWordCountMap() {
        return wordCountMap;
    }



    public static void main(String[] args) {
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        MyStorm myStorm = new MyStorm();
        executorService.submit(new MySpout(myStorm));
        executorService.submit(new MyBoltSplit(myStorm));
        executorService.submit(new MyBoltCount(myStorm));

    }



}

class MySpout implements Runnable {

    public MySpout(MyStorm myStorm) {
        this.myStorm = myStorm;
    }

    private MyStorm myStorm;
    @Override
    public void run() {
        while(true) {
            myStorm.nextTuple();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

class MyBoltSplit implements Runnable {

    public MyBoltSplit(MyStorm myStorm) {
        this.myStorm = myStorm;
    }

    private MyStorm myStorm;

    @Override
    public void run() {
        while (true) {
            try {
                String sentence = (String)myStorm.getSentenceQueue().take();
                myStorm.split(sentence);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

class MyBoltCount implements Runnable {

    public MyBoltCount(MyStorm myStorm) {
        this.myStorm = myStorm;
    }
    private MyStorm myStorm;

    @Override
    public void run() {
        while(true) {
            try {
                String word = (String)myStorm.getWordQueue().take();
                myStorm.count(word);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
