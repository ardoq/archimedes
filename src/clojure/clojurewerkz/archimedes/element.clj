(ns clojurewerkz.archimedes.element
  (:refer-clojure :exclude [keys vals assoc! dissoc! get])
  (:import [org.apache.tinkerpop.gremlin.structure Element]))

(defn get
  ([^Element elem key]
     (get elem key nil))
  ([^Element elem key not-found]
   (let [value (.property elem (name key))]
     (if (.isPresent value) (.value value) not-found))))

(defn keys
  [^Element elem]
  (set (map keyword (.keys elem))))

(defn vals
  [^Element elem]
  (set (map #(.property elem %) (.keys elem))))

(defn id-of
  [^Element elem]
  (.id elem))

(defn assoc!
  [^Element elem & kvs]
  ;;Avoids changing keys that shouldn't be changed.
  ;;Important when using types. You aren't ever going to change a
  ;;user's id for example.
  (doseq [[key value] (partition 2 kvs)]
    (let [value' (cond ;; Turn Clojure classes into something more commonly handled
                   (set? value)
                   (java.util.HashSet. value)
                   (sequential? value)
                   (java.util.ArrayList. value)
                   :else value)]
      ;; NOTE If elem is Vertex and the Graph supports 
      ;; o.a.t.g.structure.Graph.Features.VertexFeatures#FEATURE_MULTI_PROPERTIES
      ;; then we could call Vertex.property(VertexProperty$Cardinality/list, key, (nth X value), ..)
      ;; for each element. The problem then is reading - in `get` we can't see whether the
      ;; property is single or list.
      (.property elem (name key) value')))
  elem)

(defn merge!
  [^Element elem & maps]
  (doseq [d maps]
    (apply assoc! elem (if (map? d)
                         (apply concat (into [] d))
                         ;; Keep the old code, in case some old clients send
                         ;; us non-maps:
                         (flatten (into [] d)))))
  elem)

(defn dissoc!
  [^Element elem & keys]
  (doseq [key keys] (.remove (.property elem (name key))))
  elem)

(defn update!
  [^Element elem key f & args]
  (let [curr-val (get elem key)
        new-val  (apply f (cons curr-val args))]
    (assoc! elem key new-val)))

(defn clear!
  [^Element elem]
  (apply dissoc! (cons elem (keys elem))))

(comment
  ; lein try com.esotericsoftware/kryo 3.0.3 ; TinkerPop 3 uses Kryo
  (clojure.core/import 
    '(com.esotericsoftware.kryo Kryo)
    '(com.esotericsoftware.kryo.io Input Output))
  (def kryo (Kryo.))

  (clojure.core/with-open [in (Input. (clojure.java.io/input-stream "out.bin"))]
    (clojure.core/with-open [out (Output. (clojure.java.io/output-stream "out.bin"))]
      (.writeClassAndObject kryo out (java.util.ArrayList. [1 2])))
    (.readClassAndObject kryo in))

  )