/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.List;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * A component that reads a portion of the MySQL server history or current state.
 * <p>
 * A reader starts out in the {@link State#STOPPED stopped} state, and when {@link #start() started} transitions to a
 * {@link State#RUNNING} state. The reader may either complete its work or be explicitly {@link #stop() stopped}, at
 * which point the reader transitions to a {@value State#STOPPING stopping} state until all already-generated
 * {@link SourceRecord}s are consumed by the client via the {@link #poll() poll} method. Only after all records are
 * consumed does the reader transition to the {@link State#STOPPED stopped} state and call the
 * {@link #uponCompletion(Runnable)} method.
 * <p>
 * See {@link ChainedReader} if multiple {@link Reader} implementations are to be run in-sequence while keeping the
 * correct start, stop, and completion semantics.
 *
 * @author Randall Hauch
 * @see ChainedReader
 */
public interface Reader {

    /**
     * The possible states of a reader.
     */
    public static enum State {
        /**
         * The reader is stopped and static.
         */
        STOPPED,

        /**
         * The reader is running and generated records.
         */
        RUNNING,

        /**
         * The reader has completed its work or been explicitly stopped, but not all of the generated records have been
         * consumed via {@link Reader#poll() polling}.
         */
        STOPPING;
    }

    /**
     * Get the name of this reader.
     *
     * @return the reader's name; never null
     */
    public String name();

    /**
     * Get the current state of this reader.
     *
     * @return the state; never null
     */
    public State state();

    /**
     * Set the function that should be called when this reader transitions from the {@link State#STOPPING} to
     * {@link State#STOPPED} state, which is after all generated records have been consumed via the {@link #poll() poll}
     * method.
     * <p>
     * This method should only be called while the reader is in the {@link State#STOPPED} state.
     *
     * @param handler
     *            the function; may not be null
     */
    public void uponCompletion(Runnable handler);

    /**
     * Perform any initialization of the reader before being started. This method should be called exactly once before
     * {@link #start()} is called, and it should block until all initialization is completed.
     */
    public default void initialize() {
        // do nothing
    }

    /**
     * After the reader has stopped, there may still be some resources we want left available until the connector task
     * is destroyed. This method is used to clean up those remaining resources upon shutdown. This method is effectively
     * the opposite of {@link #initialize()}, performing any de-initialization of the reader entity before shutdown.
     * This method should be called exactly once after {@link #stop()} is called, and it should block until all
     * de-initialization is completed.
     */
    public default void destroy() {
        // do nothing
    }

    /**
     * Start the reader and return immediately. Once started, the {@link SourceRecord}s generated by the reader can be
     * obtained by periodically calling {@link #poll()} until that method returns {@code null}.
     * <p>
     * This method does nothing if it is already running.
     */
    public void start();

    /**
     * Stop the reader from running and transition to the {@link State#STOPPING} state until all remaining records are
     * {@link #poll() consumed}, at which point its state transitions to {@link State#STOPPED}.
     */
    public void stop();

    /**
     * Poll for the next batch of source records. This method returns {@code null} only when all records generated by
     * this reader have been processed, following the natural or explicit {@link #stop() stopping} of this reader. Note
     * that this method may block if no additional records are available but the reader may produce more, thus callers
     * should call this method continually until this method returns {@code null}.
     *
     * @return the list of source records that may or may not be empty; or {@code null} when there will be no more
     *         records because the reader has completely {@link State#STOPPED}.
     * @throws InterruptedException
     *             if this thread is interrupted while waiting for more records
     * @throws ConnectException
     *             if there is an error while this reader is running
     */
    public List<SourceRecord> poll() throws InterruptedException;
}
