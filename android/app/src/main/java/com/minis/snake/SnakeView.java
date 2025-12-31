package com.minis.snake;

import android.content.Context;
import android.graphics.Canvas;
import android.os.Handler;
import android.os.Looper;
import android.util.AttributeSet;
import android.view.View;

public class SnakeView extends View {

    private PlayManager playManager;
    private Handler handler = new Handler(Looper.getMainLooper());

    // CHANGED: Run at ~60 FPS so PlayManager can count frames
    private static final long FRAME_DELAY = 16;

    private Runnable gameLoop = new Runnable() {
        @Override
        public void run() {
            if (playManager != null) {
                playManager.update();
                invalidate();
            }
            handler.postDelayed(this, FRAME_DELAY);
        }
    };

    public SnakeView(Context context, AttributeSet attrs) {
        super(context, attrs);
    }

    public void setPlayManager(PlayManager pm) {
        this.playManager = pm;
        handler.post(gameLoop);
    }

    @Override
    protected void onDraw(Canvas canvas) {
        super.onDraw(canvas);
        if (playManager != null) {
            playManager.draw(canvas, getWidth(), getHeight());
        }
    }
}