package com.minis.snake;

import android.content.Context;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.graphics.Rect;
import com.minis.Minis;
import com.minis.R;
import android.util.Log;

import java.util.ArrayList;

public class PlayManager {

    public static final String REPLAY_FRAME = "replay:frame:";
    public static final String REPLAY_SCORE = "replay:score:";
    public static final String REPLAY_LEVEL = "replay:level:";
    public static final String REPLAY_META_LENGTH = "replay:meta:length";
    public static final String PRESS_RESET_LABEL = "PRESS RESET";
    public static final String GAME_VER_LABEL = "GAME OVER";
    public static final String SCORE_LABEL = "Score: ";
    public static final String LVL_LABEL = "Lvl: ";

    private enum State {
        ATTRACT,   // Watching replay, waiting for user
        PLAYING,   // User is playing, we are recording
        GAME_OVER  // User died, waiting 5s before going back to ATTRACT
    }
    private State currentState = State.ATTRACT;

    private final Snake snake;
    private final Bitmap mouseBitmap;
    private final Bitmap headBitmap;
    private final Bitmap bodyBitmap;
    private final Paint paint = new Paint();
    private final Minis minis;

    // GAME DATA
    private int score = 0;
    private int level = 1;
    private int thingsEaten = 0;
    private int framesLimit = 30;
    private int updateCount = 0;

    // REPLAY / RECORD DATA
    private long globalTick = 0;
    private long maxRecordedTick = 0;
    private long replayTick = 0;
    private int stateTimer = 0;

    public PlayManager(Context context, Minis minis) {
        this.minis = minis;
        snake = new Snake(20, 12);
        mouseBitmap = BitmapFactory.decodeResource(context.getResources(), R.drawable.mouse);
        headBitmap = BitmapFactory.decodeResource(context.getResources(), R.drawable.snake);
        bodyBitmap = BitmapFactory.decodeResource(context.getResources(), R.drawable.snakebody);

        // Load existing replay length
        String lenStr = minis.get(REPLAY_META_LENGTH);
        if (lenStr != null) maxRecordedTick = Long.parseLong(lenStr);

        enterAttractMode();
    }


    private void enterAttractMode() {
        currentState = State.ATTRACT;
        snake.reset();
        replayTick = 0;

        // If we have no data, maybe we just sit idle or start fresh
        if (maxRecordedTick == 0) {
            // Optional: Handle case where DB is empty (just show empty board)
        }
    }

    private void enterPlayMode() {
        currentState = State.PLAYING;
        snake.reset();

        // Reset Game Stats
        score = 0;
        level = 1;
        thingsEaten = 0;
        framesLimit = 30;
        updateCount = 0;

        // Reset Recording Stats
        globalTick = 0;
    }

    private void enterGameOverMode() {
        currentState = State.GAME_OVER;
        stateTimer = 0;

        long oldMax = maxRecordedTick;
        long newMax = globalTick;

        minis.set(REPLAY_META_LENGTH, String.valueOf(newMax));
        maxRecordedTick = newMax;

        if (newMax < oldMax) {
            // Run this in a background thread if the gap is HUGE (e.g. >10k frames)
            // But for <1000 frames, it's instant.
            garbageCollect(newMax, oldMax);
        }
    }

    public void update() {
        switch (currentState) {
            case GAME_OVER:
                stateTimer++;
                if (stateTimer > 300) { // 5 seconds
                    enterAttractMode();
                }
                break;

            case ATTRACT:
                // Replay Logic
                if (maxRecordedTick > 0) {
                    if (replayTick < maxRecordedTick) {
                        String frameData = minis.get(REPLAY_FRAME + replayTick);
                        if (frameData != null) {
                            snake.deserializeBoard(frameData);
                            // Visuals only (score/level)
                            String s = minis.get(REPLAY_SCORE + replayTick);
                            if (s != null) score = Integer.parseInt(s);
                            String l = minis.get(REPLAY_LEVEL + replayTick);
                            if (l != null) level = Integer.parseInt(l);

                            replayTick++;
                        } else {
                            replayTick = 0; // Data gap? Reset.
                        }
                    } else {
                        replayTick = 0; // Loop replay
                    }
                }
                break;

            case PLAYING:
                // Game Logic
                updateCount++;
                if (updateCount >= framesLimit) {
                    updateCount = 0;
                    Snake.Move move = snake.move();

                    if (!move.isAlive()) {
                        enterGameOverMode();
                    } else if (move.eaten()) {
                        thingsEaten++;
                        score += level * move.distance();
                        if (thingsEaten % 5 == 0) {
                            level++;
                            framesLimit = Math.max(5, framesLimit - 5);
                        }
                    }
                }

                // Recording Logic
                if (currentState == State.PLAYING) { // Double check we didn't die above
                    String state = snake.getSerializedState();
                    minis.mset(
                            REPLAY_FRAME + globalTick, state,
                            REPLAY_SCORE + globalTick, String.valueOf(score),
                            REPLAY_LEVEL + globalTick, String.valueOf(level)
                    );
                    globalTick++;
                }
                break;
        }
    }

    public void maybeReset() {
        // Any button press in ATTRACT or GAME_OVER starts the game
        if (currentState == State.ATTRACT || currentState == State.GAME_OVER) {
            enterPlayMode();
        }
    }

    public void up() { if (currentState == State.PLAYING) snake.up(); }
    public void down() { if (currentState == State.PLAYING) snake.down(); }
    public void left() { if (currentState == State.PLAYING) snake.left(); }
    public void right() { if (currentState == State.PLAYING) snake.right(); }

    public void draw(Canvas canvas, float screenWidth, float screenHeight) {
        // ... (Grid drawing logic is identical to before) ...
        int ROWS = 22;
        int COLS = 14;
        float blockSize = Math.min(screenWidth / COLS, screenHeight / ROWS);
        float offsetX = (screenWidth - (blockSize * COLS)) / 2;
        float offsetY = (screenHeight - (blockSize * ROWS)) / 2;

        paint.setColor(Color.BLACK);
        canvas.drawRect(offsetX, offsetY, offsetX + (COLS *blockSize), offsetY + (ROWS *blockSize), paint);
        Rect destRect = new Rect();
        char[][] board = snake.getBoard();
        for (int r = 0; r < board.length; r++) {
            for (int c = 0; c < board[r].length; c++) {
                float x = offsetX + (c * blockSize);
                float y = offsetY + (r * blockSize);
                char cell = board[r][c];
                if (cell == Snake.BORDER) {
                    paint.setColor(Color.LTGRAY);
                    canvas.drawRect(x, y, x + blockSize, y + blockSize, paint);
                    paint.setStyle(Paint.Style.STROKE);
                    paint.setColor(Color.BLACK);
                    canvas.drawRect(x, y, x + blockSize, y + blockSize, paint);
                    paint.setStyle(Paint.Style.FILL);
                }
                if (cell == Snake.HEAD) {
                    destRect.set((int)x, (int)y, (int)(x + blockSize), (int)(y + blockSize));
                    canvas.drawBitmap(headBitmap, null, destRect, paint);
                } else if (Snake.isSnake(cell)) {
                    destRect.set((int)x, (int)y, (int)(x + blockSize*1.2), (int)(y + blockSize*1.2));
                    canvas.drawBitmap(bodyBitmap, null, destRect, paint);
                } else if (cell == Snake.PLUS) {
                    destRect.set((int)x, (int)y, (int)(x + blockSize), (int)(y + blockSize));
                    canvas.drawBitmap(mouseBitmap, null, destRect, paint);
                } else if (cell == Snake.DEAD) {
                    paint.setColor(Color.BLUE);
                    canvas.drawRect(x, y, x + blockSize, y + blockSize, paint);
                } else if (cell >= 48 && cell < 60) {
                    paint.setColor(0xFF795548);
                    canvas.drawRect(x, y, x + blockSize, y + blockSize, paint);
                    paint.setColor(Color.WHITE);
                    paint.setTextSize(blockSize * 0.7f);
                    float textX = x + (blockSize * 0.1f);
                    float textY = y + (blockSize * 0.75f);
                    String text = (cell - 48) + "x";
                    canvas.drawText(text, textX, textY, paint);
                }
            }
        }

        // HUD
        paint.setColor(Color.WHITE);
        paint.setTextSize(60);
        canvas.drawText(SCORE_LABEL + score, 50, 100, paint);
        canvas.drawText(LVL_LABEL + level, 50, 180, paint);

        if (currentState == State.GAME_OVER) {
            paint.setColor(Color.RED);
            paint.setTextSize(100);
            // Centered text calculation (optional improvement)
            float textWidth = paint.measureText(GAME_VER_LABEL);
            canvas.drawText(GAME_VER_LABEL, (screenWidth - textWidth) / 2, screenHeight / 2, paint);

        } else if (currentState == State.ATTRACT) {
            // BLINK LOGIC: Every 500ms, toggle visibility
            if ((System.currentTimeMillis() / 500) % 2 == 0) {
                paint.setColor(Color.YELLOW);
                paint.setTextSize(80);
                float textWidth = paint.measureText(PRESS_RESET_LABEL);
                canvas.drawText(PRESS_RESET_LABEL, (screenWidth - textWidth) / 2, screenHeight / 2, paint);
            }
        }
    }

    private void garbageCollect(long startTick, long endTick) {
        // We delete in batches to avoid allocating massive String arrays
        final int BATCH_SIZE = 1000;

        var keys = new ArrayList<String>();
        long totalDeleted = 0;

        for (long i = startTick; i < endTick; i++) {
            keys.add(REPLAY_FRAME + i);
            keys.add(REPLAY_SCORE + i);
            keys.add(REPLAY_LEVEL + i);

            // If we hit our batch limit (100 frames = 300 keys)
            if (keys.size() >= BATCH_SIZE * 3) {
                totalDeleted += minis.mdel(keys.toArray(new String[0]));
                keys.clear();
            }
        }

        // Flush any remaining keys
        if (!keys.isEmpty()) {
            totalDeleted += minis.mdel(keys.toArray(new String[0]));
        }

        Log.d("MinisGC", "Pruned frames " + startTick + " to " + endTick +
                ". Total keys deleted: " + totalDeleted);
    }
}