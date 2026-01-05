package com.minis.snake;

enum Direction {
    UP(new int[] { -1, 0 }, "u"),
    DOWN(new int[] { 1, 0 }, "d"),
    LEFT(new int[] { 0, -1 }, "l"),
    RIGHT(new int[] { 0, 1 }, "r");

    Direction(int[] delta, String text) {
        this.delta = delta;
        this.text = text;
    }

    private int[] delta;
    private String text;

    public int[] getDelta() {
        return delta;
    }

    public String getText() {
        return text;
    }
}
