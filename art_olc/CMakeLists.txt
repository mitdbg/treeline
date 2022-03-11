set(art_sources
  Epoche.cpp
  Epoche.h
  Key.h
  N.cpp
  N.h
  N4.cpp
  N16.cpp
  N48.cpp
  N256.cpp
  Tree.cpp
  Tree.h)

target_sources(llsm PRIVATE ${art_sources})
target_sources(pg_llsm PRIVATE ${art_sources})
