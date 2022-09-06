(ns clojurewerkz.archimedes.element
  (:refer-clojure :exclude [keys vals assoc! dissoc! get])
  (:import [org.apache.tinkerpop.gremlin.structure Edge Element Vertex VertexProperty$Cardinality]))

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
    (let [value' (if (coll? value)
                   ;; The Graph serialization likely doesn't support Clojure coll
                   ;; classes but quite likely it does handle arrays
                   (into-array Object value)
                   value)]
      (if (instance? Vertex elem)
        (cond
          (set? value)
          (.property elem VertexProperty$Cardinality/set (name key) value' (to-array []))
          (sequential? value)
          (.property elem VertexProperty$Cardinality/list (name key) value' (to-array []))
          :else
          (.property elem VertexProperty$Cardinality/single (name key) value' (to-array [])))
        (.property elem (name key) value'))))
  elem)

(defn merge!
  [^Element elem & maps]
  (doseq [d maps]
    (apply assoc! elem (if (map? d)
                         (apply concat (into [] d))
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
